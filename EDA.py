# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory Data Analysis

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA consists of:
# MAGIC * Arrival/Departure Delay Frequency
# MAGIC * Average Arrival/Departure Delay per Month
# MAGIC * Average Arrival/Departure Delay per Airport
# MAGIC * Average Arrival/Departure Delay per Day of Week
# MAGIC * Average Arrival/Departure Delay per Day of Month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS FREQUENCY, ARR_DELAY
# MAGIC FROM dscc202_group02_db.bronze_airports_cleaned
# MAGIC GROUP BY ARR_DELAY
# MAGIC SORT BY ARR_DELAY

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS FREQUENCY, DEP_DELAY
# MAGIC FROM dscc202_group02_db.bronze_airports_cleaned
# MAGIC GROUP BY DEP_DELAY
# MAGIC SORT BY DEP_DELAY

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ARR_DELAY) AS avg_arr_delay, AVG(DEP_DELAY) AS avg_dep_delay, MONTH
# MAGIC FROM dscc202_group02_db.bronze_airports_cleaned
# MAGIC GROUP BY MONTH
# MAGIC SORT BY MONTH

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ARR_DELAY) AS avg_arr_delay, AVG(DEP_DELAY) AS avg_dep_delay, ORIGIN
# MAGIC FROM dscc202_group02_db.bronze_airports_cleaned
# MAGIC GROUP BY ORIGIN
# MAGIC SORT BY ORIGIN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ARR_DELAY) AS avg_arr_delay, AVG(DEP_DELAY) AS avg_dep_delay, DAY_OF_WEEK
# MAGIC FROM dscc202_group02_db.bronze_airports_cleaned
# MAGIC GROUP BY DAY_OF_WEEK
# MAGIC SORT BY DAY_OF_WEEK

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ARR_DELAY) AS avg_arr_delay, AVG(DEP_DELAY) AS avg_dep_delay, DAY_OF_MONTH
# MAGIC FROM dscc202_group02_db.bronze_airports_cleaned
# MAGIC GROUP BY DAY_OF_MONTH
# MAGIC SORT BY DAY_OF_MONTH