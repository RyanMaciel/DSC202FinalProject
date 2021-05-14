# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory Data Analysis

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Draws a heatmap of the correlation between all the columns
# and sorts the correlations of the columns with a SPECIFIC column_name 

def plot_correlation(df):
  import matplotlib.pyplot as plt
  import seaborn as sns
  import pandas as pd
  
  df_pd = df.toPandas()
  df_pd.shape
  
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
  
  return corr

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA consists of:
# MAGIC * Arrival/Departure Delay Frequency
# MAGIC * Average Arrival/Departure Delay per Month
# MAGIC * Average Arrival/Departure Delay per Airport
# MAGIC * Average Arrival/Departure Delay per Day of Week
# MAGIC * Average Arrival/Departure Delay per Day of Month
# MAGIC * Correlation between Arrival Delay and the other variables
# MAGIC * Correlation between Simplified Weather Data and Arrival/Departure Delay for NYC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS FREQUENCY, ARR_DELAY
# MAGIC FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3
# MAGIC GROUP BY ARR_DELAY
# MAGIC SORT BY ARR_DELAY

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS FREQUENCY, DEP_DELAY
# MAGIC FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3
# MAGIC GROUP BY DEP_DELAY
# MAGIC SORT BY DEP_DELAY

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ARR_DELAY) AS avg_arr_delay, AVG(DEP_DELAY) AS avg_dep_delay, DAY_OF_WEEK
# MAGIC FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3
# MAGIC GROUP BY DAY_OF_WEEK
# MAGIC SORT BY DAY_OF_WEEK

# COMMAND ----------

# MAGIC %sql
# MAGIC -- https://dwgeek.com/spark-sql-date-and-timestamp-functions-and-examples.html/ -> to convert from TIMESTAMP to MONTH
# MAGIC SELECT AVG(ARR_DELAY) AS avg_arr_delay, AVG(DEP_DELAY) AS avg_dep_delay, month(
# MAGIC SCHEDULED_DEP_TIME) AS MONTH
# MAGIC FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3
# MAGIC GROUP BY MONTH
# MAGIC SORT BY MONTH

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ARR_DELAY) AS avg_arr_delay, AVG(DEP_DELAY) AS avg_dep_delay, ORIGIN
# MAGIC FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3  
# MAGIC WHERE ORIGIN IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
# MAGIC GROUP BY ORIGIN
# MAGIC SORT BY ORIGIN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ARR_DELAY) AS avg_arr_delay, AVG(DEP_DELAY) AS avg_dep_delay, dayofmonth(SCHEDULED_DEP_TIME) AS DAY_OF_MONTH
# MAGIC FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3
# MAGIC GROUP BY DAY_OF_MONTH
# MAGIC SORT BY DAY_OF_MONTH

# COMMAND ----------

flightDF = spark.sql("SELECT * FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3")

# COMMAND ----------

corr = plot_correlation(flightDF)
corr["ARR_DELAY"].sort_values()

# COMMAND ----------

# Join Flight and Weather data for NYC and JFK based on year, month, day and hour.
flight_and_weather_DF = spark.sql("""
  SELECT *
  FROM dscc202_group02_db.bronze_airport_weather_join_imputed
""")

# COMMAND ----------

display(flight_and_weather_DF)

# COMMAND ----------

corr = plot_correlation(flight_and_weather_DF)
corr["ARR_DELAY"].sort_values()

# COMMAND ----------

from pyspark.sql.functions import *
dayDF = (flight_and_weather_DF.withColumn("day", date_trunc('day', "SCHEDULED_DEP_TIME"))
         .withColumn("gone", col("orgin_tot_precip_mm").isNull())
        ).groupby("day").agg(count("gone")).orderBy("day")
display(dayDF)


# COMMAND ----------

display(dayDF.agg(min("day"), max("day")))

# COMMAND ----------

dbutils.notebook.exit("Success")