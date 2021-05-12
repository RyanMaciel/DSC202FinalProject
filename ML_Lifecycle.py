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

(trainDF, testDF) = df.randomSplit([0.8,0.2], seed=42)

feature_list = []
for col in df.columns:
    if col == 'ARR_DELAY' or col in ['SCHEDULED_DEP_TIME', 'SCHEDULED_ARR_TIME', 'WHEELS_OFF', 'WHEELS_ON']:
        continue
    else:
        feature_list.append(col)
        
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


rf = RandomForestRegressor(labelCol="ARR_DELAY", maxDepth=maxDepth, numTrees=numTrees, seed=42)
pipeline = Pipeline(stages=[stringIndexer, vecAssembler, rf])

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Begin Run, Train Model

# COMMAND ----------

import mlflow
import mlflow.spark
import pandas as pd

with mlflow.start_run(run_name="random-forest") as run:
  # Log params: Num Trees and Max Depth
  mlflow.log_param("num_trees", rf.getNumTrees())
  mlflow.log_param("max_depth", rf.getMaxDepth())
 
  # Log model
  pipelineModel = pipeline.fit(trainDF)
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

display(predDF.select(['ARR_DELAY', 'prediction']))

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Register and Track Model

# COMMAND ----------

import uuid

runID = runs[0].info.run_uuid
model_name = f"flight_delay_{uuid.uuid4().hex[:10]}"
model_uri = "runs:/{run_id}/model".format(run_id=runID)

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)
model_details

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
runs = client.search_runs(run.info.experiment_id,
                          order_by=["attributes.start_time desc"], 
                          max_results=1)
run_id = runs[0].info.run_id
runs[0].data.metrics

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Load Model and Predict Arrival Delay

# COMMAND ----------

# Load saved model with MLflow
pipelineModel = mlflow.spark.load_model(f"runs:/{run_id}/model")
predDF = pipelineModel.transform(testDF)
y_true_pred_df = predDF.select(["ARR_DELAY", "prediction"])
display(y_true_pred_df)

# COMMAND ----------

