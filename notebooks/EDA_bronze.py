# Databricks notebook source
# MAGIC %sql
# MAGIC select * from  dscc202_db.bronze_weather limit 20;

# COMMAND ----------

import pandas_profiling
from pandas_profiling.utils.cache import cache_file

# COMMAND ----------

df=spark.sql("select * from  dscc202_db.bronze_weather limit 1000")
# df.head()

# COMMAND ----------

displayHTML(pandas_profiling.ProfileReport(df.toPandas()).html)