# Databricks notebook source
airport_code = "SFO"
training_start_date = "2018-01-01"
training_end_date = "2019-01-01"
inference_date = "2019-03-16"

# COMMAND ----------

model_name = "rfr_{0}_{1}_{2}_{3}".format(airport_code, training_start_date, training_end_date, inference_date)

# COMMAND ----------

# stage can be "Production" or "Staging"
def get_logged_model(model_name, stage):
  from mlflow.tracking import MlflowClient
  client = MlflowClient()
  for mv in client.search_model_versions(f"name='{model_name}'"):
    if dict(mv)['current_stage'] == stage:
      run_id = dict(mv)["run_id"]
      return "runs:/" + run_id + "/{0}_rfr".format(airport_code)

# COMMAND ----------

logged_model_production = get_logged_model(model_name, "Production")
logged_model_staging = get_logged_model(model_name, "Staging")

# COMMAND ----------

import mlflow

loaded_model_production = mlflow.pyfunc.load_model(logged_model_production)
loaded_model_staging = mlflow.pyfunc.load_model(logged_model_staging)

# COMMAND ----------

# Predict on a Pandas DataFrame.
import pandas as pd
from pyspark.sql.functions import col, to_date
df_inference = spark.sql("""
SELECT * FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3
WHERE ORIGIN IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
AND DEST = "{}"
""".format(airport_code))
df_inference =  (df_inference.filter(df_inference.DEST == airport_code)
                  .filter(to_date(col("SCHEDULED_DEP_TIME")) == inference_date))
df_inference_pd = df_inference.toPandas()
df_inference_pd["Predictions_Production"] = loaded_model_production.predict(df_inference.toPandas())
df_inference_pd["Predictions_Staging"] = loaded_model_staging.predict(df_inference.toPandas())

# COMMAND ----------

df_inference_pd[["Predictions_Production", "Predictions_Staging"]]