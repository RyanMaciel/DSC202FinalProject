# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Preprocess Non-Numeric Data

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC USE dscc202_group02_db

# COMMAND ----------

# MAGIC %md
# MAGIC # Assemble Prediction Pipeline and Fit Model

# COMMAND ----------

df = spark.sql("""
SELECT * 
FROM bronze_air_traffic_cleaned_v3 
WHERE ORIGIN IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
AND DEST IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
""")
df = df.drop("OP_CARRIER_FL_NUM", "DIV_AIRPORT_LANDINGS")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Create Prediction Pipeline

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import lit

# COMMAND ----------

(trainDF, testDF) = df.randomSplit([0.8,0.2], seed=42)

# COMMAND ----------

# Encoding categorical columns using a StringIndexer
# https://spark.apache.org/docs/latest/ml-features#stringindexer
categoricalCols = [field for (field, dType) in trainDF.dtypes if dType=="string"]
indexOutputCols = [x + "Index" for x in categoricalCols]
stringIndexer = StringIndexer(inputCols=categoricalCols,
                              outputCols=indexOutputCols, 
                              handleInvalid="skip")
numericCols = [field for (field, dType) in trainDF.dtypes if (dType=="double" and \
                                                              field not in ['SCHEDULED_DEP_TIME', 'SCHEDULED_ARR_TIME', 'WHEELS_OFF', 'WHEELS_ON'] and \
                                                              field != 'ARR_DELAY')]
assemblerInputs = indexOutputCols + numericCols
vecAssembler = VectorAssembler(inputCols=assemblerInputs,
                              outputCol="features")


# COMMAND ----------

# create a random forest regressor
rf = RandomForestRegressor(featuresCol = "features", labelCol="ARR_DELAY", maxDepth=5, numTrees=100, seed=42)
pipeline = Pipeline(stages=[stringIndexer, vecAssembler, rf])

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Begin Run, Train Model

# COMMAND ----------

import mlflow
import mlflow.spark
import pandas as pd

with mlflow.start_run(run_name="random-forest-regressor") as run:
  # Log params: Num Trees and Max Depth
  mlflow.log_param("num_trees", rf.getNumTrees())
  mlflow.log_param("max_depth", rf.getMaxDepth())
  # Log model
  pipelineModel = pipeline.fit(trainDF)
#   it is at this point where the pipeline "modifies" the training dataset and vectorizes it
  mlflow.spark.log_model(pipelineModel, "model")

  # Log metrics: RMSE and R2
  predDF = pipelineModel.transform(testDF)
  regressionEvaluator = RegressionEvaluator(predictionCol="prediction", 
                                            labelCol="ARR_DELAY")
  rmse = regressionEvaluator.setMetricName("rmse").evaluate(predDF)
  r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
  mlflow.log_metrics({"rmse": rmse, "r2": r2})

  # Log artifact: Feature Importance Scores
  rfModel = pipelineModel.stages[-1]
  pandasDF = (pd.DataFrame(list(zip(vecAssembler.getInputCols(), 
                                    rfModel.featureImportances)), 
                          columns=["feature", "importance"])
              .sort_values(by="importance", ascending=False))
  # First write to local filesystem, then tell MLflow where to find that file
  pandasDF.to_csv("feature-importance.csv", index=False)
  mlflow.log_artifact("feature-importance.csv")


# COMMAND ----------

predDF.select(['ARR_DELAY', 'prediction']).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Register and Track Model

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
runs = client.search_runs(run.info.experiment_id,
                          order_by=["attributes.start_time desc"], 
                          max_results=1)
run_id = runs[0].info.run_id
runs[0].data.metrics

# COMMAND ----------

import uuid

runID = runs[0].info.run_uuid
model_name = f"flight_delay_{uuid.uuid4().hex[:10]}"
model_uri = "runs:/{run_id}/model".format(run_id=runID)

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)
model_details

# COMMAND ----------

# MAGIC %md
# MAGIC At this point look at the models tab and you can see the model registered.

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Load Model and Predict Arrival Delay

# COMMAND ----------

# Load saved model with MLflow
pipelineModel = mlflow.spark.load_model(f"runs:/{run_id}/model")

# COMMAND ----------

predDF = pipelineModel.transform(testDF)
y_true_pred_df = predDF.select(["ARR_DELAY", "prediction"])
y_true_pred_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Trying to generalize everything and connect to airport code and training start and end date

# COMMAND ----------

# variables considered as parameters from the main notebook
airport_code = "SFO"
training_start_date = "2018/01/01"
training_end_date = "2019-01-01"
inference_date = "2019-03-16"

# COMMAND ----------

def train_model(df_orig):
  from pyspark.sql.functions import col
# The following dataframe contains the destination airport and the training dates range. They are used for training and testing a dataset in the training dates range.
# This is where we measure the performance from.
  df = (df_orig.filter(df_orig.DEST == airport_code)
        .filter(col("SCHEDULED_DEP_TIME").
                between(pd.to_datetime(training_start_date), 
                        pd.to_datetime(training_end_date))))
#   the following dataframe contains only the inference date and the destination airport. It is used for predicting the actual values
  df_inference = (df_orig.filter(df_orig.DEST == airport_code)
                  .filter(to_date(col("SCHEDULED_DEP_TIME")) == inference_date))
  dest = airport_code
  (trainDF, testDF) = df.randomSplit([0.8,0.2], seed=42)
  with mlflow.start_run(run_name="flights-randomforest-with-regressors-{0}".format(dest)) as run:
    rf = RandomForestRegressor(featuresCol = "features", labelCol="ARR_DELAY", maxDepth=5, numTrees=100, seed=42)
    pipeline = Pipeline(stages=[stringIndexer, vecAssembler, rf])
    mlflow.log_param("num_trees", rf.getNumTrees())
    mlflow.log_param("max_depth", rf.getMaxDepth())
    # Log model
    pipelineModel = pipeline.fit(trainDF)
    # it is at this point where the pipeline "modifies" the training dataset and vectorizes it
    mlflow.spark.log_model(pipelineModel,
                           artifact_path="{0}_rfr".format(airport_code),
                           registered_model_name = "rfr_{0}_{1}_{2}_{3}".format(airport_code, training_start_date, training_end_date, inference_date))
    
    tags = {"training_start_date": training_start_date, "training_end_date": training_end_date}
    mlflow.set_tags(tags)

    # Log metrics: RMSE and R2
    predDF = pipelineModel.transform(testDF)
    regressionEvaluator = RegressionEvaluator(predictionCol="prediction", 
                                              labelCol="ARR_DELAY")
    rmse = regressionEvaluator.setMetricName("rmse").evaluate(predDF)
    r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
    mlflow.log_metrics({"rmse": rmse, "r2": r2})
    
    # move this latest version of the model to the Staging if there is a production version
    # else register it as the production version
    model_name = "rfr_{0}_{1}_{2}_{3}".format(airport_code, training_start_date, training_end_date, inference_date)
    model_version = dict(client.search_model_versions(f"name='{model_name}'")[0])['version']
    model_stage = "Production"
    for mv in client.search_model_versions(f"name='{model_name}'"):
      if dict(mv)['current_stage'] == 'Staging':
          # Archive the currently staged model
          client.transition_model_version_stage(
              name=dict(mv)['name'],
              version=dict(mv)['version'],
              stage="Archived"
          )
          model_stage = "Staging"
      elif dict(mv)['current_stage'] == 'Production':
          model_stage = "Staging"
    # move the model to the appropriate stage.
    client.transition_model_version_stage(
        name=model_name,
        version=model_version,
        stage=model_stage
    )
        
  predicted_inference_DF = pipelineModel.transform(df_inference)
#   the idea now is to return the predicted delay for each model version and save these things in a table such as the one in notebook 06 RandomForest with Time & Weather.
  return predicted_inference_DF

# COMMAND ----------

inputs = spark.sql("""
SELECT * 
FROM bronze_air_traffic_cleaned_v3 
WHERE ORIGIN IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
AND DEST IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
""")

# COMMAND ----------

display(inputs)

# COMMAND ----------

predicted_inference_DF = train_model(inputs)

# COMMAND ----------

