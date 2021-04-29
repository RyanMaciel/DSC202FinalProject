# Databricks notebook source
v = "1.6M"

# COMMAND ----------

result = v.str.extract('(\d+\.*\d+)').astype(float)

# COMMAND ----------

