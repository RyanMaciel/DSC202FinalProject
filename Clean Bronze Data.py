# Databricks notebook source
# MAGIC %md
# MAGIC # Cleaning the Bronze Data
# MAGIC * Drop Low Quality Columns based on number of null values
# MAGIC * Drop Logically Useless (or duplicated) Data

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC USE dscc202_db

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Low Quality Columns - AC

# COMMAND ----------

# Load Bronze Data into spark DataFrame
df = spark.sql("SELECT * FROM bronze_air_traffic")

# COMMAND ----------

# Print the SQL query for the next code cell
print("SELECT")
for c in df.columns:
  print(f"       COUNT(*)-COUNT({c}) As {c}", end='')
  if c!=df.columns[-1]:
    print(",")
  else:
    print("\nFROM bronze_air_traffic")

# COMMAND ----------

# Get the number of null values in each column
null_val_counts = spark.sql(
"""
SELECT
       COUNT(*)-COUNT(YEAR) As YEAR,
       COUNT(*)-COUNT(QUARTER) As QUARTER,
       COUNT(*)-COUNT(MONTH) As MONTH,
       COUNT(*)-COUNT(DAY_OF_MONTH) As DAY_OF_MONTH,
       COUNT(*)-COUNT(DAY_OF_WEEK) As DAY_OF_WEEK,
       COUNT(*)-COUNT(FL_DATE) As FL_DATE,
       COUNT(*)-COUNT(OP_UNIQUE_CARRIER) As OP_UNIQUE_CARRIER,
       COUNT(*)-COUNT(OP_CARRIER_AIRLINE_ID) As OP_CARRIER_AIRLINE_ID,
       COUNT(*)-COUNT(OP_CARRIER) As OP_CARRIER,
       COUNT(*)-COUNT(TAIL_NUM) As TAIL_NUM,
       COUNT(*)-COUNT(OP_CARRIER_FL_NUM) As OP_CARRIER_FL_NUM,
       COUNT(*)-COUNT(ORIGIN_AIRPORT_ID) As ORIGIN_AIRPORT_ID,
       COUNT(*)-COUNT(ORIGIN_AIRPORT_SEQ_ID) As ORIGIN_AIRPORT_SEQ_ID,
       COUNT(*)-COUNT(ORIGIN_CITY_MARKET_ID) As ORIGIN_CITY_MARKET_ID,
       COUNT(*)-COUNT(ORIGIN) As ORIGIN,
       COUNT(*)-COUNT(ORIGIN_CITY_NAME) As ORIGIN_CITY_NAME,
       COUNT(*)-COUNT(ORIGIN_STATE_ABR) As ORIGIN_STATE_ABR,
       COUNT(*)-COUNT(ORIGIN_STATE_FIPS) As ORIGIN_STATE_FIPS,
       COUNT(*)-COUNT(ORIGIN_STATE_NM) As ORIGIN_STATE_NM,
       COUNT(*)-COUNT(ORIGIN_WAC) As ORIGIN_WAC,
       COUNT(*)-COUNT(DEST_AIRPORT_ID) As DEST_AIRPORT_ID,
       COUNT(*)-COUNT(DEST_AIRPORT_SEQ_ID) As DEST_AIRPORT_SEQ_ID,
       COUNT(*)-COUNT(DEST_CITY_MARKET_ID) As DEST_CITY_MARKET_ID,
       COUNT(*)-COUNT(DEST) As DEST,
       COUNT(*)-COUNT(DEST_CITY_NAME) As DEST_CITY_NAME,
       COUNT(*)-COUNT(DEST_STATE_ABR) As DEST_STATE_ABR,
       COUNT(*)-COUNT(DEST_STATE_FIPS) As DEST_STATE_FIPS,
       COUNT(*)-COUNT(DEST_STATE_NM) As DEST_STATE_NM,
       COUNT(*)-COUNT(DEST_WAC) As DEST_WAC,
       COUNT(*)-COUNT(CRS_DEP_TIME) As CRS_DEP_TIME,
       COUNT(*)-COUNT(DEP_TIME) As DEP_TIME,
       COUNT(*)-COUNT(DEP_DELAY) As DEP_DELAY,
       COUNT(*)-COUNT(DEP_DELAY_NEW) As DEP_DELAY_NEW,
       COUNT(*)-COUNT(DEP_DEL15) As DEP_DEL15,
       COUNT(*)-COUNT(DEP_DELAY_GROUP) As DEP_DELAY_GROUP,
       COUNT(*)-COUNT(DEP_TIME_BLK) As DEP_TIME_BLK,
       COUNT(*)-COUNT(TAXI_OUT) As TAXI_OUT,
       COUNT(*)-COUNT(WHEELS_OFF) As WHEELS_OFF,
       COUNT(*)-COUNT(WHEELS_ON) As WHEELS_ON,
       COUNT(*)-COUNT(TAXI_IN) As TAXI_IN,
       COUNT(*)-COUNT(CRS_ARR_TIME) As CRS_ARR_TIME,
       COUNT(*)-COUNT(ARR_TIME) As ARR_TIME,
       COUNT(*)-COUNT(ARR_DELAY) As ARR_DELAY,
       COUNT(*)-COUNT(ARR_DELAY_NEW) As ARR_DELAY_NEW,
       COUNT(*)-COUNT(ARR_DEL15) As ARR_DEL15,
       COUNT(*)-COUNT(ARR_DELAY_GROUP) As ARR_DELAY_GROUP,
       COUNT(*)-COUNT(ARR_TIME_BLK) As ARR_TIME_BLK,
       COUNT(*)-COUNT(CANCELLED) As CANCELLED,
       COUNT(*)-COUNT(CANCELLATION_CODE) As CANCELLATION_CODE,
       COUNT(*)-COUNT(DIVERTED) As DIVERTED,
       COUNT(*)-COUNT(CRS_ELAPSED_TIME) As CRS_ELAPSED_TIME,
       COUNT(*)-COUNT(ACTUAL_ELAPSED_TIME) As ACTUAL_ELAPSED_TIME,
       COUNT(*)-COUNT(AIR_TIME) As AIR_TIME,
       COUNT(*)-COUNT(FLIGHTS) As FLIGHTS,
       COUNT(*)-COUNT(DISTANCE) As DISTANCE,
       COUNT(*)-COUNT(DISTANCE_GROUP) As DISTANCE_GROUP,
       COUNT(*)-COUNT(CARRIER_DELAY) As CARRIER_DELAY,
       COUNT(*)-COUNT(WEATHER_DELAY) As WEATHER_DELAY,
       COUNT(*)-COUNT(NAS_DELAY) As NAS_DELAY,
       COUNT(*)-COUNT(SECURITY_DELAY) As SECURITY_DELAY,
       COUNT(*)-COUNT(LATE_AIRCRAFT_DELAY) As LATE_AIRCRAFT_DELAY,
       COUNT(*)-COUNT(FIRST_DEP_TIME) As FIRST_DEP_TIME,
       COUNT(*)-COUNT(TOTAL_ADD_GTIME) As TOTAL_ADD_GTIME,
       COUNT(*)-COUNT(LONGEST_ADD_GTIME) As LONGEST_ADD_GTIME,
       COUNT(*)-COUNT(DIV_AIRPORT_LANDINGS) As DIV_AIRPORT_LANDINGS,
       COUNT(*)-COUNT(DIV_REACHED_DEST) As DIV_REACHED_DEST,
       COUNT(*)-COUNT(DIV_ACTUAL_ELAPSED_TIME) As DIV_ACTUAL_ELAPSED_TIME,
       COUNT(*)-COUNT(DIV_ARR_DELAY) As DIV_ARR_DELAY,
       COUNT(*)-COUNT(DIV_DISTANCE) As DIV_DISTANCE,
       COUNT(*)-COUNT(DIV1_AIRPORT) As DIV1_AIRPORT,
       COUNT(*)-COUNT(DIV1_AIRPORT_ID) As DIV1_AIRPORT_ID,
       COUNT(*)-COUNT(DIV1_AIRPORT_SEQ_ID) As DIV1_AIRPORT_SEQ_ID,
       COUNT(*)-COUNT(DIV1_WHEELS_ON) As DIV1_WHEELS_ON,
       COUNT(*)-COUNT(DIV1_TOTAL_GTIME) As DIV1_TOTAL_GTIME,
       COUNT(*)-COUNT(DIV1_LONGEST_GTIME) As DIV1_LONGEST_GTIME,
       COUNT(*)-COUNT(DIV1_WHEELS_OFF) As DIV1_WHEELS_OFF,
       COUNT(*)-COUNT(DIV1_TAIL_NUM) As DIV1_TAIL_NUM,
       COUNT(*)-COUNT(DIV2_AIRPORT) As DIV2_AIRPORT,
       COUNT(*)-COUNT(DIV2_AIRPORT_ID) As DIV2_AIRPORT_ID,
       COUNT(*)-COUNT(DIV2_AIRPORT_SEQ_ID) As DIV2_AIRPORT_SEQ_ID,
       COUNT(*)-COUNT(DIV2_WHEELS_ON) As DIV2_WHEELS_ON,
       COUNT(*)-COUNT(DIV2_TOTAL_GTIME) As DIV2_TOTAL_GTIME,
       COUNT(*)-COUNT(DIV2_LONGEST_GTIME) As DIV2_LONGEST_GTIME,
       COUNT(*)-COUNT(DIV2_WHEELS_OFF) As DIV2_WHEELS_OFF,
       COUNT(*)-COUNT(DIV2_TAIL_NUM) As DIV2_TAIL_NUM,
       COUNT(*)-COUNT(DIV3_AIRPORT) As DIV3_AIRPORT,
       COUNT(*)-COUNT(DIV3_AIRPORT_ID) As DIV3_AIRPORT_ID,
       COUNT(*)-COUNT(DIV3_AIRPORT_SEQ_ID) As DIV3_AIRPORT_SEQ_ID,
       COUNT(*)-COUNT(DIV3_WHEELS_ON) As DIV3_WHEELS_ON,
       COUNT(*)-COUNT(DIV3_TOTAL_GTIME) As DIV3_TOTAL_GTIME,
       COUNT(*)-COUNT(DIV3_LONGEST_GTIME) As DIV3_LONGEST_GTIME,
       COUNT(*)-COUNT(DIV3_WHEELS_OFF) As DIV3_WHEELS_OFF,
       COUNT(*)-COUNT(DIV3_TAIL_NUM) As DIV3_TAIL_NUM,
       COUNT(*)-COUNT(DIV4_AIRPORT) As DIV4_AIRPORT,
       COUNT(*)-COUNT(DIV4_AIRPORT_ID) As DIV4_AIRPORT_ID,
       COUNT(*)-COUNT(DIV4_AIRPORT_SEQ_ID) As DIV4_AIRPORT_SEQ_ID,
       COUNT(*)-COUNT(DIV4_WHEELS_ON) As DIV4_WHEELS_ON,
       COUNT(*)-COUNT(DIV4_TOTAL_GTIME) As DIV4_TOTAL_GTIME,
       COUNT(*)-COUNT(DIV4_LONGEST_GTIME) As DIV4_LONGEST_GTIME,
       COUNT(*)-COUNT(DIV4_WHEELS_OFF) As DIV4_WHEELS_OFF,
       COUNT(*)-COUNT(DIV4_TAIL_NUM) As DIV4_TAIL_NUM,
       COUNT(*)-COUNT(DIV5_AIRPORT) As DIV5_AIRPORT,
       COUNT(*)-COUNT(DIV5_AIRPORT_ID) As DIV5_AIRPORT_ID,
       COUNT(*)-COUNT(DIV5_AIRPORT_SEQ_ID) As DIV5_AIRPORT_SEQ_ID,
       COUNT(*)-COUNT(DIV5_WHEELS_ON) As DIV5_WHEELS_ON,
       COUNT(*)-COUNT(DIV5_TOTAL_GTIME) As DIV5_TOTAL_GTIME,
       COUNT(*)-COUNT(DIV5_LONGEST_GTIME) As DIV5_LONGEST_GTIME,
       COUNT(*)-COUNT(DIV5_WHEELS_OFF) As DIV5_WHEELS_OFF,
       COUNT(*)-COUNT(DIV5_TAIL_NUM) As DIV5_TAIL_NUM
FROM bronze_air_traffic""" )

# COMMAND ----------

# Collect Rows, index the first (contains null value counts), and store as dictionary
null_val_counts_dict = null_val_counts.collect()[0].asDict()
null_val_counts_dict

# COMMAND ----------

# Create null value percentage dictionary
nrows, ncols = df.count(), len(df.columns)
null_val_pcts = {col_name:nvc/nrows for col_name, nvc in null_val_counts_dict.items()}
print("Rows:",nrows,"\nColumns:",ncols,"\nNull Value Percentages:",null_val_pcts)

# COMMAND ----------

# Get names of columns we are keeping
thresh = 0.20
nullCols20 = [] # columns with > 20% null values
to_keep = []
for c in df.columns:
  if null_val_pcts[c] > thresh:
    nullCols20.append(c)
  else:
    to_keep.append(c)
    
print("Num to drop:", len(nullCols20), "\nNum to Keep", len(to_keep), "\nCol Names:", nullCols20)

# COMMAND ----------

# Drop columns above threshold of null values and write to table
df_dropNulls20 = df.select(to_keep)

from pyspark.sql.utils import AnalysisException
try:
  df_dropNulls20.write.saveAsTable("dscc202_group02_db.bronze_air_traffic_dropNulls20")
except AnalysisException:
  print("Table already exists")

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Query table to assert that it was written successfully */
# MAGIC 
# MAGIC SELECT * FROM dscc202_group02_db.bronze_air_traffic_dropNulls20 LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Logically Useless data

# COMMAND ----------

# MAGIC %sql
# MAGIC USE dscc202_group02_db

# COMMAND ----------

# Ryan - Load data
logicalCleanDF = spark.sql("SELECT * FROM bronze_air_traffic_dropNulls20")

# COMMAND ----------

# Ryan - drop columns unneeded.
from pyspark.sql.functions import *

# DEST_AIRPORT_ID, DEST_AIRPORT_SEQ_ID, DEST_CITY_MARKET_ID, DEST, DEST_CITY_NAME, DEST_STATE_ABR, DEST_STATE_FIPS,
# DEST_STATE_NM, DEST_WAC all contain very overlapping data. I suggest we keep only DEST, as the others are redundant.


logicalCleanDF = logicalCleanDF.drop("DEST_AIRPORT_ID", "DEST_AIRPORT_SEQ_ID", "DEST_CITY_MARKET_ID", "DEST_CITY_NAME", "DEST_STATE_ABR", "DEST_STATE_FIPS", "DEST_STATE_NM", "DEST_WAC");

# Similarly for origin:
logicalCleanDF = logicalCleanDF.drop("ORIGIN_AIRPORT_ID", "ORIGIN_AIRPORT_SEQ_ID", "ORIGIN_CITY_MARKET_ID", "ORIGIN_CITY_NAME", "ORIGIN_STATE_ABR", "ORIGIN_STATE_FIPS", "ORIGIN_STATE_NM", "ORIGIN_WAC")

# It looks like fields DEP_DELAY_NEW and ARR_DELAY_NEW min at 0, whereas the non-new ones can be negative. We probably want that negative data.
logicalCleanDF = logicalCleanDF.drop("DEP_DELAY_NEW", "ARR_DELAY_NEW");

#If we are keeping month, drop quarter
logicalCleanDF = logicalCleanDF.drop("QUARTER", "ARR_DELAY_NEW");

#OP_UNIQUE_CARRIER, OP_CARRIER_AIRLINE_ID, OP_CARRIER are all very similar only keep one. I think OP_UNIQUE_CARRIER is the best choice
logicalCleanDF = logicalCleanDF.drop("OP_CARRIER_AIRLINE_ID", "OP_CARRIER");

#Flights is 1 for every row.
logicalCleanDF = logicalCleanDF.drop("FLIGHTS");

#display(logicalCleanDF.select(countDistinct("DIVERTED")))
#logicalCleanDF.groupBy('DIVERTED').count().show()


display(logicalCleanDF);

# COMMAND ----------

#refining DF by dropping more columns -- Nishith

#The diagram professor gave us in the main notebook says we only need to use flight number to show estimate so following fields seems irrelevant for our analysis
logicalCleanDF = logicalCleanDF.drop("TAIL_NUM");
logicalCleanDF = logicalCleanDF.drop("OP_UNIQUE_CARRIER");

#CRS_DEP_TIME and CRS_ARR_TIME is the actual scheduled flight departure and arrival we need so renamed it to make it more understandable
logicalCleanDF = logicalCleanDF.withColumnRenamed("CRS_DEP_TIME", "SCHEDULED_DEP_TIME")
logicalCleanDF = logicalCleanDF.withColumnRenamed("CRS_ARR_TIME", "SCHEDULED_ARR_TIME")

#these field flag if delay/arrival is or more than 15 mins. this doesn't seem necessary given we have DEP_DELAY and ARR_DELAY
logicalCleanDF = logicalCleanDF.drop("DEP_DEL15");
logicalCleanDF = logicalCleanDF.drop("ARR_DEL15");

#I believe these fields are just grouping each row based on delay time, arrival time and flight distance. It doesn't seem relevant for our analysis
logicalCleanDF = logicalCleanDF.drop("DEP_DELAY_GROUP");
logicalCleanDF = logicalCleanDF.drop("ARR_DELAY_GROUP");
logicalCleanDF = logicalCleanDF.drop("DISTANCE_GROUP");


display(logicalCleanDF);

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
try:
  logicalCleanDF.write.saveAsTable("dscc202_group02_db.bronze_air_traffic_cleaned")
except AnalysisException:
  print("Table already exists")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM dscc202_group02_db.bronze_air_traffic_cleaned
# MAGIC LIMIT 10

# COMMAND ----------

airportsOfInterestDF = spark.sql("""
SELECT * 
FROM dscc202_group02_db.bronze_air_traffic_cleaned 
WHERE ORIGIN IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
OR DEST IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
""")

# COMMAND ----------

print((airportsOfInterestDF.count(), len(airportsOfInterestDF.columns)))

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
try:
  airportsOfInterestDF.write.saveAsTable("dscc202_group02_db.bronze_airports_cleaned")
except AnalysisException:
  print("Table already exists")

# COMMAND ----------

df = sqlContext.sql("SELECT * FROM dscc202_group02_db.bronze_airports_cleaned LIMIT 9000")

# COMMAND ----------

import tensorflow_data_validation as tfdv
import pandas as pd
import pandas_profiling

displayHTML(pandas_profiling.ProfileReport(df).html)

# COMMAND ----------

from sklearn.model_selection import train_test_split
import tensorflow_data_validation as tfdv
from tensorflow_data_validation.utils.display_util import get_statistics_html
import warnings

warnings.filterwarnings("ignore", message=r"Passing", category=FutureWarning)

TRAIN, TEST = train_test_split(df, test_size=0.2, random_state=42)
EVAL, SERVE = train_test_split(TEST, test_size=0.5, random_state=42)

# COMMAND ----------

stats_train=tfdv.generate_statistics_from_dataframe(dataframe=TRAIN)
stats_eval=tfdv.generate_statistics_from_dataframe(dataframe=EVAL)

# COMMAND ----------

displayHTML(get_statistics_html(stats_train))

# COMMAND ----------

schema = tfdv.infer_schema(statistics=stats_train)
tfdv.display_schema(schema=schema)

# COMMAND ----------

# Compare evaluation data with training data
displayHTML(get_statistics_html(lhs_statistics=stats_eval, rhs_statistics=stats_train,
                          lhs_name='EVAL_DATASET', rhs_name='TRAIN_DATASET'))

# COMMAND ----------

