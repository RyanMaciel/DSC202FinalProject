# Databricks notebook source
from datetime import datetime as dt

dbutils.widgets.dropdown("Airport Code", "JFK", ["JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH"])
dbutils.widgets.text('Training Start Date', "2018-01-01")
dbutils.widgets.text('Training End Date', "2018-02-01")
dbutils.widgets.text('Inference Date', (dt.strptime(str(dbutils.widgets.get('Training End Date')), "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"))

airport_code = str(dbutils.widgets.get('Airport Code'))
training_start_date = str(dbutils.widgets.get('Training Start Date'))
training_end_date = str(dbutils.widgets.get('Training End Date'))
inference_date = str(dbutils.widgets.get('Inference Date'))

print(airport_code)
print(training_start_date)
print(training_end_date)
print(inference_date)

# training_start_date = "2018-01-01"
# training_end_date = "2018-02-01"
# dbutils.widgets.text('Inference Date', "2019-03-15")
# inference_date = str(dbutils.widgets.get('Inference Date'))

dbutils.widgets.dropdown('Promote model?', "No", ["Yes", "No"])
promote_model = True if str(dbutils.widgets.get('Promote model?')) == "Yes" else False

# COMMAND ----------

from datetime import datetime, timedelta
start = datetime.strptime(inference_date, "%Y-%m-%d")
monitoring_date = start - timedelta(days=1) 
print(monitoring_date)

# COMMAND ----------

model_name = "rfr_{0}_{1}_{2}".format(airport_code, training_start_date, training_end_date)

# COMMAND ----------

prod_version = None
stage_version = None

# COMMAND ----------

# stage can be "Production" or "Staging"
def get_logged_model(model_name, stage):
  from mlflow.tracking import MlflowClient
  client = MlflowClient()
  for mv in client.search_model_versions(f"name='{model_name}'"):
    if dict(mv)['current_stage'] == stage:
      run_id = dict(mv)["run_id"]
      return "runs:/" + run_id + "/{0}_rfr".format(airport_code), dict(mv)['version']

# COMMAND ----------

logged_model_production, prod_version = get_logged_model(model_name, "Production")
logged_model_staging, stage_version = get_logged_model(model_name, "Staging")

# COMMAND ----------

print(logged_model_production, prod_version)
print(logged_model_staging, stage_version)

# COMMAND ----------

import mlflow

loaded_model_production = mlflow.pyfunc.load_model(logged_model_production)
loaded_model_staging = mlflow.pyfunc.load_model(logged_model_staging)

# COMMAND ----------

# Predict on a Pandas DataFrame.
import pandas as pd
from pyspark.sql.functions import col, to_date
df_monitoring = spark.sql("""
SELECT * FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3
WHERE ORIGIN IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
AND DEST = "{}"
""".format(airport_code))
df_monitoring =  (df_monitoring.filter(df_monitoring.DEST == airport_code)
                  .filter(to_date(col("SCHEDULED_DEP_TIME")) == monitoring_date))
df_monitoring_pd = df_monitoring.toPandas()
df_monitoring_pd["Predictions_Production"] = loaded_model_production.predict(df_monitoring.toPandas())
df_monitoring_pd["Predictions_Staging"] = loaded_model_staging.predict(df_monitoring.toPandas())

# COMMAND ----------

df_monitoring_pd['residual_staging'] = df_monitoring_pd["ARR_DELAY"] - df_monitoring_pd["Predictions_Staging"]
df_monitoring_pd['residual_production'] = df_monitoring_pd["ARR_DELAY"] - df_monitoring_pd["Predictions_Production"]

# COMMAND ----------

df_monitoring_pd = df_monitoring_pd.sort_values(by = "SCHEDULED_DEP_TIME")

# COMMAND ----------

import plotly.express as px

fig = px.line(df_monitoring_pd, x = "SCHEDULED_DEP_TIME", y = ["ARR_DELAY", "Predictions_Production", "Predictions_Staging"], title = "Scatterplot of {0} with Production and Staging model for date {1} with respect to actual delay".format(airport_code, inference_date))
fig.show()

# COMMAND ----------

import plotly.express as px

fig = px.scatter(
    df_monitoring_pd, x='ARR_DELAY', y=['residual_staging', 'residual_production'],
    marginal_y='violin',
    title="Residual plot for date {} on airport {}".format(monitoring_date, airport_code)
)
fig.show()

# COMMAND ----------

# promote staging to production using widget
from mlflow.tracking import MlflowClient

client = MlflowClient()
if promote_model and loaded_model_production is not None and loaded_model_staging is not None:
  # Archive the production model
  client.transition_model_version_stage(
      name=model_name,
      version=prod_version,
      stage="Archived"
  )

  # Staging --> Production
  client.transition_model_version_stage(
      name=model_name,
      version=stage_version,
      stage="Production"
  )

# COMMAND ----------

dbutils.notebook.exit("Success")