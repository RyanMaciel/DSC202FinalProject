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

df = spark.sql("SELECT * FROM bronze_air_traffic_cleaned_v3")
df = df.drop("OP_CARRIER_FL_NUM", "DIV_AIRPORT_LANDINGS")
display(df)

# COMMAND ----------

strings_used = ["ORIGIN", "DEST", 'DAY_OF_WEEK']

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer

stage_string = [StringIndexer(inputCol= c, outputCol= c+"_string_encoded") for c in strings_used]
stage_one_hot = [OneHotEncoder(inputCol= c+"_string_encoded", outputCol= c+ "_one_hot") for c in strings_used]

ppl = Pipeline(stages= stage_string + stage_one_hot)
df = ppl.fit(df).transform(df)

# COMMAND ----------

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

X = df.select([
  'SCHEDULED_DEP_TIME',
  'TAXI_OUT',
  'WHEELS_OFF',
  'WHEELS_ON',
  'TAXI_IN',
  'SCHEDULED_ARR_TIME',
  'ARR_DELAY',
  'CANCELLED',
  'DIVERTED',
  'CRS_ELAPSED_TIME',
  'ACTUAL_ELAPSED_TIME',
  'AIR_TIME',
  'DISTANCE',
  'ORIGIN_one_hot',
  'DEST_one_hot',
  'DAY_OF_WEEK_one_hot'
])
y = df.select('DEP_DELAY')

# COMMAND ----------

X_train, X_test = X.randomSplit([0.9, 0.1], seed=12345)
y_train, y_test = y.randomSplit([0.9, 0.1], seed=12345)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Model Selection and Training

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature
from pyspark.ml.regression import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# COMMAND ----------

display(X_train)

# COMMAND ----------

X_train_pd = X_train.toPandas()
X = X_train_pd.values
X_copy = X_train_pd.values

# COMMAND ----------

with mlflow.start_run(run_name="Flight_Predictions_RF") as run:
  # Initialize model
  model = RandomForestRegressor(featuresCol="indexedFeatures")
  # Train model and predict labels
  model.fit(X, y_train)
  signature = infer_signature(X_copy, y_train)
  
  y_pred = model.predict(X_test.toPandas().values)

  # Log 
  mlflow.sklearn.log_model(model, 
                           artifact_path="{}_rf_model".format(airport_id),
                          register_model_name="{}-reg-rf-model".format(airport_id),
                          signature=signature)
  tags = {"start_of_training": start_date, "end_of_training":end_date}
  # Create metrics
  mse = mean_squared_error(y_test, y_pred)
  # Log metrics
  mlflow.log_metric("mse", mse)
  info = run.info
    
  print(f"Inside MLflow Run\n  - Run ID: {info.run_uuid}\n  - Experiment ID: {info.experiment_id}")

# COMMAND ----------

