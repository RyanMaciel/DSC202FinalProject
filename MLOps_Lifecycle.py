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

# MAGIC %run ./includes/configuration

# COMMAND ----------

airport_code = str(dbutils.widgets.get('Airport Code'))
training_start_date = str(dbutils.widgets.get('Training Start Date'))
training_end_date = str(dbutils.widgets.get('Training End Date'))
inference_date = str(dbutils.widgets.get('Inference Date'))

# airport_code = "ORD"
# training_start_date = "2018-01-01"
# training_end_date = "2018-02-01"
# inference_date = "2018-05-01"

# COMMAND ----------

df = spark.sql("""
SELECT * 
FROM silver_airport_weather_join_imputed
""")

display(df.take(5))

# COMMAND ----------

# returns assemblers for a dataframe; i.e. converts the 
def load_assemblers(df, train_on):
  
  from pyspark.ml.feature import StringIndexer, VectorAssembler
  # Encoding categorical columns using a StringIndexer
  # https://spark.apache.org/docs/latest/ml-features#stringindexer
  categoricalCols = [field for (field, dType) in df.dtypes if dType=="string"]
  indexOutputCols = [x + "Index" for x in categoricalCols]
  stringIndexer = StringIndexer(inputCols=categoricalCols,
                                outputCols=indexOutputCols, 
                                handleInvalid="skip")
  numericCols = None
  if train_on == "ARR":
    numericCols = [field for (field, dType) in df.dtypes if (dType=="double" and field != 'ARR_DELAY')]
  elif train_on == "DEP":
    cols_to_drop_if_departure = ["TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "ARR_DELAY", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME"]
    numericCols = [field for (field, dType) in df.dtypes if (dType=="double" and field not in cols_to_drop_if_departure and field != "DEP_DELAY")]
  assemblerInputs = indexOutputCols + numericCols
  
  vecAssembler = VectorAssembler(inputCols=assemblerInputs,
                                outputCol="features")

  return stringIndexer, vecAssembler

# COMMAND ----------

# creates a run and saves it in under a model
# a model will have one staging and one production version (only the first one is production)
# train_on can be "ARR" or "DEP"
def train_model(df_orig, maxDepth, numTrees, stringIndexer, vecAssembler, train_on):
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
  
  
# The following dataframe contains the airport and the training dates range. They are used for training and testing a dataset in the training dates range.
# This is where we measure the performance from.

  df = None
  if train_on == "ARR":
    df = (df_orig.filter(df_orig.DEST == airport_code)
          .filter(col("SCHEDULED_ARR_TIME").
                  between(pd.to_datetime(training_start_date), 
                          pd.to_datetime(training_end_date))))
  
  
#     display(df_orig)
  elif train_on == "DEP":
  
    cols_to_drop_if_departure = ["TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "ARR_DELAY", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME"]
    df = (df_orig.filter(df_orig.ORIGIN == airport_code)
          .filter(col("SCHEDULED_DEP_TIME").
                  between(pd.to_datetime(training_start_date), 
                          pd.to_datetime(training_end_date)))
         .drop(*cols_to_drop_if_departure))
#     display(df.take(2))

#   print("DF = ")
#   display(df)
  (trainDF, testDF) = df.randomSplit([0.8,0.2], seed=42)
  
  with mlflow.start_run(run_name="flights-randomforest-with-regressors-{0}_arr_del".format(airport_code)) as run:
    rf = None
    if train_on == "ARR":
        rf = RandomForestRegressor(featuresCol = "features", labelCol="ARR_DELAY", maxDepth=maxDepth, numTrees=numTrees)
    elif train_on == "DEP":
      rf = RandomForestRegressor(featuresCol = "features", labelCol="DEP_DELAY", maxDepth=maxDepth, numTrees=numTrees)
    pipeline = Pipeline(stages=[stringIndexer, vecAssembler, rf])
    mlflow.log_param("num_trees", rf.getNumTrees())
    mlflow.log_param("max_depth", rf.getMaxDepth())
    
#     print(train_on)
#     display(trainDF)
    # Log model
    pipelineModel = pipeline.fit(trainDF)
    # it is at this point where the pipeline "modifies" the training dataset and vectorizes it
    mlflow.spark.log_model(pipelineModel,
                           "{0}_rfr_{1}".format(airport_code, train_on))
    
    tags = {"training_start_date": training_start_date, "training_end_date": training_end_date}
    mlflow.set_tags(tags)

    # Log metrics: RMSE and R2
    predDF = pipelineModel.transform(testDF)
    regressionEvaluator = None
    if train_on == "ARR":
      regressionEvaluator = RegressionEvaluator(predictionCol="prediction", 
                                              labelCol="ARR_DELAY")
    elif train_on == "DEP":
      regressionEvaluator = RegressionEvaluator(predictionCol="prediction", 
                                              labelCol="DEP_DELAY")
      
    rmse = regressionEvaluator.setMetricName("rmse").evaluate(predDF)
    r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
    mlflow.log_metrics({"rmse": rmse, "r2": r2})
    
    
  client = MlflowClient()
  runs = client.search_runs(run.info.experiment_id,
                          order_by=["attributes.start_time desc"], 
                          max_results=1)
  runID = runs[0].info.run_uuid
  model_name = "rfr_{0}_{1}_{2}_{3}".format(airport_code, training_start_date, training_end_date, train_on)
  model_uri = "runs:/{run_id}/{code}_rfr_{arr_del}".format(run_id=runID, code = airport_code, arr_del = train_on)
  print("model_name = ", model_name, "model_uri = ", model_uri)
  model_details = mlflow.register_model(model_uri=model_uri, name=model_name)
  print("REGISTERED")
   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating multiple models

# COMMAND ----------

depth_trees = [(5, 100), (5, 90), (5, 80)] # , (5, 70), (6, 90), (6, 80), (6, 70), (6, 60)]
for train_on in ["DEP", "ARR"]:
  stringIndexer, vecAssembler = load_assemblers(df, train_on)
  for depth, trees in depth_trees:
    train_model(df, depth, trees, stringIndexer, vecAssembler, train_on)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finding the best model

# COMMAND ----------

# https://www.mlflow.org/docs/latest/python_api/mlflow.tracking.html

from pprint import pprint
from mlflow.tracking import MlflowClient
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructType, StructField

client = MlflowClient()

# we save the best run version for both DEP and ARR in an array
best_run_version = []
second_best_run_version = []

for train_on in ["DEP", "ARR"]:
  model_name = "rfr_{0}_{1}_{2}_{3}".format(airport_code, training_start_date, training_end_date, train_on)

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
  # https://stackoverflow.com/questions/40661859/getting-the-first-value-from-spark-sql-row
  best_run = runs_df.take(1)[0]
  best_run_version.append(best_run[1])
  second_best_run = runs_df.take(2)[1]
  second_best_run_version.append(second_best_run[1])

# COMMAND ----------

for i, train_on in enumerate(["DEP", "ARR"]):
  # archive the current production version
  model_name = "rfr_{0}_{1}_{2}_{3}".format(airport_code, training_start_date, training_end_date, train_on)
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
        version=best_run_version[i],
        stage="Production"
  )
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
        version=second_best_run_version[i],
        stage="Staging"
  )

# COMMAND ----------

# stage can be "Production" or "Staging"
def get_logged_model(model_name, stage, train_on):
  for mv in client.search_model_versions(f"name='{model_name}'"):
    if dict(mv)['current_stage'] == stage:
      run_id = dict(mv)["run_id"]
      return "runs:/" + run_id + "/{0}_rfr_{1}".format(airport_code, train_on)

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, to_date

model_name_ARR = "rfr_{0}_{1}_{2}_ARR".format(airport_code, training_start_date, training_end_date)
model_name_DEP = "rfr_{0}_{1}_{2}_DEP".format(airport_code, training_start_date, training_end_date)


logged_model_ARR = get_logged_model(model_name_ARR, "Production", "ARR")
logged_model_DEP = get_logged_model(model_name_DEP, "Production", "DEP")


loaded_model_ARR = mlflow.pyfunc.load_model(logged_model_ARR)
loaded_model_DEP = mlflow.pyfunc.load_model(logged_model_DEP)

df_inference_ARR = (df.filter(df.DEST == airport_code)
                  .filter(to_date(col("SCHEDULED_ARR_TIME")) == inference_date))


cols_to_drop_if_departure = ["DEP_DELAY", "TAXI_OUT", "TAXI_IN", "WHEELS_OFF", "WHEELS_ON", "ARR_DELAY", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME"]
df_inference_DEP = (df.filter(df.ORIGIN == airport_code)
                    .filter(to_date(col("SCHEDULED_DEP_TIME")) == inference_date)
                    .drop(*cols_to_drop_if_departure))

# Predict on a Pandas DataFrame.
df_inference_ARR_pd = df_inference_ARR.toPandas()
df_inference_ARR_pd["ARR_Preds"] = loaded_model_ARR.predict(df_inference_ARR.toPandas())
df_inference_DEP_pd = df_inference_DEP.toPandas()
df_inference_DEP_pd["DEP_Preds"] = loaded_model_DEP.predict(df_inference_DEP.toPandas())


# COMMAND ----------

df_inference_ARR = spark.createDataFrame(df_inference_ARR_pd)
df_inference_DEP = spark.createDataFrame(df_inference_DEP_pd)

# COMMAND ----------

df_inference_ARR

# COMMAND ----------

df_inference_ARR.write.mode("overwrite").saveAsTable("dscc202_group02_db.gold_table_ARR")
df_inference_DEP.write.mode("overwrite").saveAsTable("dscc202_group02_db.gold_table_DEP")

# COMMAND ----------

dbutils.notebook.exit("Success")