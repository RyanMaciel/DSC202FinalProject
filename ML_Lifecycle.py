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

df = spark.sql("""
SELECT * 
FROM bronze_air_traffic_cleaned_v3 
WHERE ORIGIN IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
AND DEST IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
""")
df = df.drop("OP_CARRIER_FL_NUM", "DIV_AIRPORT_LANDINGS")
display(df)

# COMMAND ----------

strings_used = ["ORIGIN", "DEST", 'DAY_OF_WEEK']

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer

stage_string = [StringIndexer(inputCol= c, outputCol= c+"_string_encoded") for c in strings_used]
# stage_one_hot = [OneHotEncoder(inputCol= c+"_string_encoded", outputCol= c+ "_one_hot") for c in strings_used]

# ppl = Pipeline(stages= stage_string + stage_one_hot)
ppl = Pipeline(stages= stage_string)
df = ppl.fit(df).transform(df)

# COMMAND ----------

df = df.drop("ORIGIN", "DEST", "DAY_OF_WEEK")

# COMMAND ----------

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Model Selection and Training

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler, VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from sklearn.metrics import mean_squared_error

# COMMAND ----------

# https://www.silect.is/blog/random-forest-models-in-spark-ml/
feature_list = []
for col in df.columns:
    if col == 'ARR_DELAY' or col in ['SCHEDULED_DEP_TIME', 'SCHEDULED_ARR_TIME', 'WHEELS_OFF', 'WHEELS_ON']:
        continue
    else:
        feature_list.append(col)
assembler = VectorAssembler(inputCols=feature_list, outputCol="features")

# COMMAND ----------

df_assembled = assembler.transform(df)
df_assembled = df_assembled.drop("SCHEDULED_DEP_TIME", "SCHEDULED_ARR_TIME", "WHEELS_OFF", "WHEELS_ON")
for feature in feature_list:
  df_assembled = df_assembled.drop(feature)

# COMMAND ----------

print(feature_list)

# COMMAND ----------

df_assembled.show(5)

# COMMAND ----------

(trainingData, testData) = df_assembled.randomSplit([0.8, 0.2])

# COMMAND ----------

# testing without ML Flow

rf = RandomForestRegressor(featuresCol = "features", labelCol = "ARR_DELAY", maxDepth = maxDepth)
#   pipeline = Pipeline(stages=[assembler, rf])
  # Train model and predict labels
model = rf.fit(trainingData)
predictions = model.transform(testData)
  
#   Logging the model
#   mlflow.sklearn.log_model(rf,
#                            artifact_path="rf_regression_model")
#   Logging metrics
evaluator = RegressionEvaluator(labelCol="ARR_DELAY", predictionCol="features", metricName="rmse")
rmse = evaluator.evaluate(predictions)
#   mlfow.log_metric("rmse",rmse)
print(rmse)

# COMMAND ----------

with mlflow.start_run(run_name="Flight_Predictions_RF_regressor") as run:
  # Initialize model
  maxDepth = 4
  rf = RandomForestRegressor(featuresCol = "features", labelCol = "ARR_DELAY", maxDepth = maxDepth)
#   pipeline = Pipeline(stages=[assembler, rf])
  # Train model and predict labels
  model = rf.fit(trainingData)
  predictions = model.transform(testData)
  
#   Logging the model
  mlflow.sklearn.log_model(rf,
                           artifact_path="rf_regression_model")
#   Logging metrics
  evaluator = RegressionEvaluator(labelCol="ARR_DELAY", predictionCol="features", metricName="rmse")
  rmse = evaluator.evaluate(predictions)
  mlfow.log_metric("rmse",rmse)
  
#   info = run.info
    
#   print(f"Inside MLflow Run\n  - Run ID: {info.run_uuid}\n  - Experiment ID: {info.experiment_id}")

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

