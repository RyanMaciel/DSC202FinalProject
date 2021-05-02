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

'''%sql
SELECT AVG(ARR_DELAY) AS avg_arr_delay, AVG(DEP_DELAY) AS avg_dep_delay, MONTH
FROM dscc202_group02_db.bronze_airports_cleaned
GROUP BY MONTH
SORT BY MONTH'''

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

'''%sql
SELECT AVG(ARR_DELAY) AS avg_arr_delay, AVG(DEP_DELAY) AS avg_dep_delay, DAY_OF_MONTH
FROM dscc202_group02_db.bronze_airports_cleaned
GROUP BY DAY_OF_MONTH
SORT BY DAY_OF_MONTH'''

# COMMAND ----------

df = spark.sql("SELECT * FROM dscc202_group02_db.bronze_airports_cleaned")

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# COMMAND ----------

df_pd = df.toPandas()
df_pd.shape

# COMMAND ----------

# Drop non-numeric columns (which is why the plot is smaller than 20x20)
corr = df_pd.corr()
ax = sns.heatmap(
    corr, 
    vmin=-1, vmax=1, center=0,
    cmap=sns.diverging_palette(20, 120, n=100),
    square=True
)
ax.set_xticklabels(
    ax.get_xticklabels(),
    rotation=45,
    horizontalalignment='right'
);

# COMMAND ----------

