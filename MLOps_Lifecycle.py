# Databricks notebook source
# MAGIC %md
# MAGIC ## The notebook takes the following 4 attributes:
# MAGIC * airport_code
# MAGIC * training_start_date
# MAGIC * training_end_date
# MAGIC * inference_date

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

from datetime import datetime as dt
from datetime import timedelta
# airport_code = "SFO"
# training_start_date = "2018-01-01"
# training_end_date = "2019-01-01"
# inference_date = "2019-03-16"

# dbutils.widgets.dropdown("00.Airport_Code", "JFK", ["JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH"])
# dbutils.widgets.text('01.training_start_date', "2018-01-01")
# dbutils.widgets.text('02.training_end_date', "2019-03-15")
# dbutils.widgets.text('03.inference_date', (dt.strptime(str(dbutils.widgets.get('02.training_end_date')), "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"))

airport_code = dbutils.widgets.get("00.Airport_Code")
training_start_date = dbutils.widgets.get("01.training_start_date")
training_end_date = dbutils.widgets.get("02.training_end_date")
inference_date = dbutils.widgets.get("03.inference_date")


print(airport_code)


# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------



airport_code = str(dbutils.widgets.get('Airport Code'))
training_start_date = str(dbutils.widgets.get('Training Start Date'))
training_end_date = str(dbutils.widgets.get('Training End Date'))
inference_date = str(dbutils.widgets.get('Inference Date'))


# COMMAND ----------

df = spark.sql("""
SELECT * 
FROM bronze_airport_weather_join 
WHERE ORIGIN IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
AND DEST IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
""")
df = df.drop('orgin_tot_precip_mm', 'dest_tot_precip_mm', 'dest_avg_wnd_mps', 'dest_avg_vis_m', 'dest_avg_slp_hpa', 'dest_avg_dewpt_f', 'orgin_avg_wnd_mps', 'orgin_avg_vis_m', 'orgin_avg_slp_hpa')



# COMMAND ----------


# returns assemblers for a dataframe; i.e. converts the 
def load_assemblers(df):
  
  from pyspark.ml.feature import StringIndexer, VectorAssembler
  # Encoding categorical columns using a StringIndexer
  # https://spark.apache.org/docs/latest/ml-features#stringindexer
  categoricalCols = [field for (field, dType) in df.dtypes if dType=="string"]
  indexOutputCols = [x + "Index" for x in categoricalCols]
  stringIndexer = StringIndexer(inputCols=categoricalCols,
                                outputCols=indexOutputCols, 
                                handleInvalid="skip")
  numericCols = [field for (field, dType) in df.dtypes if (dType=="double" and field != 'ARR_DELAY')]
  
  assemblerInputs = indexOutputCols + numericCols
  print(assemblerInputs);
  vecAssembler = VectorAssembler(inputCols=assemblerInputs,
                                outputCol="features")

  return stringIndexer, vecAssembler

# COMMAND ----------

# creates a run and saves it in under a model
# a model will have one staging and one production version (only the first one is production)
def train_model(df_orig, maxDepth, numTrees, stringIndexer, vecAssembler):
  from pyspark.sql.functions import col, to_date
  import mlflow
  import mlflow.spark
  import pandas as pd
  import uuid
  from pyspark.ml import Pipeline
  from pyspark.ml.feature import StringIndexer, VectorAssembler
  from pyspark.ml.regression import RandomForestRegressor
  from pyspark.ml.evaluation import RegressionEvaluator
  from pyspark.sql.functions import lit
  from mlflow.tracking import MlflowClient

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
    rf = RandomForestRegressor(featuresCol = "features", labelCol="ARR_DELAY", maxDepth=maxDepth, numTrees=numTrees)
    pipeline = Pipeline(stages=[stringIndexer, vecAssembler, rf])
    mlflow.log_param("num_trees", rf.getNumTrees())
    mlflow.log_param("max_depth", rf.getMaxDepth())
    # Log model
    pipelineModel = pipeline.fit(trainDF)
    # it is at this point where the pipeline "modifies" the training dataset and vectorizes it
    mlflow.spark.log_model(pipelineModel,
                           "{0}_rfr".format(airport_code))
    
    tags = {"training_start_date": training_start_date, "training_end_date": training_end_date}
    mlflow.set_tags(tags)

    # Log metrics: RMSE and R2
    predDF = pipelineModel.transform(testDF)
    regressionEvaluator = RegressionEvaluator(predictionCol="prediction", 
                                              labelCol="ARR_DELAY")
    rmse = regressionEvaluator.setMetricName("rmse").evaluate(predDF)
    r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
    mlflow.log_metrics({"rmse": rmse, "r2": r2})
    
    
  client = MlflowClient()
  runs = client.search_runs(run.info.experiment_id,
                          order_by=["attributes.start_time desc"], 
                          max_results=1)
  runID = runs[0].info.run_uuid
  model_name = "rfr_{0}_{1}_{2}".format(airport_code, training_start_date, training_end_date)
  model_uri = "runs:/{run_id}/{code}_rfr".format(run_id=runID, code = dest)
  model_details = mlflow.register_model(model_uri=model_uri, name=model_name)
#   model_details
    # move this latest version of the model to the Staging if there is a production version
    # else register it as the production version
    
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
   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating multiple models

# COMMAND ----------

depth_trees = [(5, 100), (5, 90), (5, 80), (5, 70), (6, 90), (6, 80), (6, 70), (6, 60)]
stringIndexer, vecAssembler = load_assemblers(df)
for depth, trees in depth_trees[0:1]:
  train_model(df, depth, trees, stringIndexer, vecAssembler)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finding the best model

# COMMAND ----------

# https://www.mlflow.org/docs/latest/python_api/mlflow.tracking.html

from pprint import pprint
from mlflow.tracking import MlflowClient
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructType, StructField

client = MlflowClient()
model_name = "rfr_{0}_{1}_{2}".format(airport_code, training_start_date, training_end_date)

runs_df_schema = StructType([ \
    StructField("run_id",StringType(),True), \
    StructField("version",StringType(),True), \
    StructField("r2",DoubleType(),True), \
    StructField("rmse",DoubleType(),True), \
  ])
runs_df_data = []
for mv in client.search_model_versions(f"name='{model_name}'"):
  run_id = dict(mv)["run_id"]
  run_version = dict(mv)["version"]
  run = client.get_run(run_id)
  runs_df_data.append((run_id, run_version, run.data.metrics["r2"], run.data.metrics["rmse"]))

  runs_df = spark.createDataFrame(data=runs_df_data,schema=runs_df_schema)
  runs_df = runs_df.sort("rmse", "r2")
display(runs_df)

# COMMAND ----------

# https://stackoverflow.com/questions/40661859/getting-the-first-value-from-spark-sql-row
best_run = runs_df.take(1)[0]
best_run_id, best_run_version = best_run[0], best_run[1]

# COMMAND ----------

second_best_run = runs_df.take(2)[1]
second_best_run_id, second_best_run_version = second_best_run[0], second_best_run[1]

# COMMAND ----------

# archive the current production version
for mv in client.search_model_versions(f"name='{model_name}'"):
  if dict(mv)['current_stage'] == 'Production':
      # Archive the current production model
      client.transition_model_version_stage(
          name=dict(mv)['name'],
          version=dict(mv)['version'],
          stage="Archived"
      )
# and set the best model version to production
client.transition_model_version_stage(
      name=model_name,
      version=best_run_version,
      stage="Production"
)

# COMMAND ----------

# Do the same, but for a staging model
# archive the current production version
for mv in client.search_model_versions(f"name='{model_name}'"):
  if dict(mv)['current_stage'] == 'Staging':
      # Archive the currently staged model
      client.transition_model_version_stage(
          name=dict(mv)['name'],
          version=dict(mv)['version'],
          stage="Archived"
      )
# and set the best model version to production
client.transition_model_version_stage(
      name=model_name,
      version=second_best_run_version,
      stage="Staging"
)

# COMMAND ----------

#display(df_inference)

# COMMAND ----------

# stage can be "Production" or "Staging"
def get_logged_model(model_name, stage):
  for mv in client.search_model_versions(f"name='{model_name}'"):
    if dict(mv)['current_stage'] == stage:
      run_id = dict(mv)["run_id"]
      return "runs:/" + run_id + "/{0}_rfr".format(airport_code)
  
logged_model = get_logged_model(model_name, "Production")

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, to_date

loaded_model = mlflow.pyfunc.load_model(logged_model)
df_inference = (df.filter(df.DEST == airport_code)
                  .filter(to_date(col("SCHEDULED_DEP_TIME")) == inference_date))

# Predict on a Pandas DataFrame.
df_inference_pd = df_inference.toPandas()
df_inference_pd["Predictions"] = loaded_model.predict(df_inference.toPandas())

# COMMAND ----------

df_inference_pd[["ORIGIN", "DEST", "OP_CARRIER_FL_NUM", "SCHEDULED_DEP_TIME", "Predictions"]]

# COMMAND ----------

dbutils.notebook.exit("Success")
