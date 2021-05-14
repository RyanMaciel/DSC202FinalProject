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

from pyspark.sql.functions import *

# COMMAND ----------

def shape(sparkDf):
  return (sparkDf.count(), len(sparkDf.columns))
def printShape(sparkDf):
  print("Rows:",sparkDf.count(), "Cols:", len(sparkDf.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Low Quality Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC USE dscc202_db

# COMMAND ----------

# Load Bronze Data into spark DataFrame
df = spark.sql("SELECT * FROM bronze_air_traffic")
printShape(df)

# COMMAND ----------

df.write.format("delta").save(BASE_DELTA_PATH+"/airport_source/")

streamingdf = readstream(BASE_DELTA_PATH+"/airport_source/")


# COMMAND ----------

# Print the SQL query for the next code cell
print("SELECT")
for c in df.columns:
  print(f"       (COUNT(*)-COUNT({c})) / COUNT(*) As {c}", end='')
  if c!=df.columns[-1]:
    print(",")
  else:
    print("\nFROM bronze_air_traffic")

# COMMAND ----------

# Get the number of null values in each column
null_val_counts = spark.sql(
"""
SELECT
       (COUNT(*)-COUNT(YEAR)) / COUNT(*) As YEAR,
       (COUNT(*)-COUNT(QUARTER)) / COUNT(*) As QUARTER,
       (COUNT(*)-COUNT(MONTH)) / COUNT(*) As MONTH,
       (COUNT(*)-COUNT(DAY_OF_MONTH)) / COUNT(*) As DAY_OF_MONTH,
       (COUNT(*)-COUNT(DAY_OF_WEEK)) / COUNT(*) As DAY_OF_WEEK,
       (COUNT(*)-COUNT(FL_DATE)) / COUNT(*) As FL_DATE,
       (COUNT(*)-COUNT(OP_UNIQUE_CARRIER)) / COUNT(*) As OP_UNIQUE_CARRIER,
       (COUNT(*)-COUNT(OP_CARRIER_AIRLINE_ID)) / COUNT(*) As OP_CARRIER_AIRLINE_ID,
       (COUNT(*)-COUNT(OP_CARRIER)) / COUNT(*) As OP_CARRIER,
       (COUNT(*)-COUNT(TAIL_NUM)) / COUNT(*) As TAIL_NUM,
       (COUNT(*)-COUNT(OP_CARRIER_FL_NUM)) / COUNT(*) As OP_CARRIER_FL_NUM,
       (COUNT(*)-COUNT(ORIGIN_AIRPORT_ID)) / COUNT(*) As ORIGIN_AIRPORT_ID,
       (COUNT(*)-COUNT(ORIGIN_AIRPORT_SEQ_ID)) / COUNT(*) As ORIGIN_AIRPORT_SEQ_ID,
       (COUNT(*)-COUNT(ORIGIN_CITY_MARKET_ID)) / COUNT(*) As ORIGIN_CITY_MARKET_ID,
       (COUNT(*)-COUNT(ORIGIN)) / COUNT(*) As ORIGIN,
       (COUNT(*)-COUNT(ORIGIN_CITY_NAME)) / COUNT(*) As ORIGIN_CITY_NAME,
       (COUNT(*)-COUNT(ORIGIN_STATE_ABR)) / COUNT(*) As ORIGIN_STATE_ABR,
       (COUNT(*)-COUNT(ORIGIN_STATE_FIPS)) / COUNT(*) As ORIGIN_STATE_FIPS,
       (COUNT(*)-COUNT(ORIGIN_STATE_NM)) / COUNT(*) As ORIGIN_STATE_NM,
       (COUNT(*)-COUNT(ORIGIN_WAC)) / COUNT(*) As ORIGIN_WAC,
       (COUNT(*)-COUNT(DEST_AIRPORT_ID)) / COUNT(*) As DEST_AIRPORT_ID,
       (COUNT(*)-COUNT(DEST_AIRPORT_SEQ_ID)) / COUNT(*) As DEST_AIRPORT_SEQ_ID,
       (COUNT(*)-COUNT(DEST_CITY_MARKET_ID)) / COUNT(*) As DEST_CITY_MARKET_ID,
       (COUNT(*)-COUNT(DEST)) / COUNT(*) As DEST,
       (COUNT(*)-COUNT(DEST_CITY_NAME)) / COUNT(*) As DEST_CITY_NAME,
       (COUNT(*)-COUNT(DEST_STATE_ABR)) / COUNT(*) As DEST_STATE_ABR,
       (COUNT(*)-COUNT(DEST_STATE_FIPS)) / COUNT(*) As DEST_STATE_FIPS,
       (COUNT(*)-COUNT(DEST_STATE_NM)) / COUNT(*) As DEST_STATE_NM,
       (COUNT(*)-COUNT(DEST_WAC)) / COUNT(*) As DEST_WAC,
       (COUNT(*)-COUNT(CRS_DEP_TIME)) / COUNT(*) As CRS_DEP_TIME,
       (COUNT(*)-COUNT(DEP_TIME)) / COUNT(*) As DEP_TIME,
       (COUNT(*)-COUNT(DEP_DELAY)) / COUNT(*) As DEP_DELAY,
       (COUNT(*)-COUNT(DEP_DELAY_NEW)) / COUNT(*) As DEP_DELAY_NEW,
       (COUNT(*)-COUNT(DEP_DEL15)) / COUNT(*) As DEP_DEL15,
       (COUNT(*)-COUNT(DEP_DELAY_GROUP)) / COUNT(*) As DEP_DELAY_GROUP,
       (COUNT(*)-COUNT(DEP_TIME_BLK)) / COUNT(*) As DEP_TIME_BLK,
       (COUNT(*)-COUNT(TAXI_OUT)) / COUNT(*) As TAXI_OUT,
       (COUNT(*)-COUNT(WHEELS_OFF)) / COUNT(*) As WHEELS_OFF,
       (COUNT(*)-COUNT(WHEELS_ON)) / COUNT(*) As WHEELS_ON,
       (COUNT(*)-COUNT(TAXI_IN)) / COUNT(*) As TAXI_IN,
       (COUNT(*)-COUNT(CRS_ARR_TIME)) / COUNT(*) As CRS_ARR_TIME,
       (COUNT(*)-COUNT(ARR_TIME)) / COUNT(*) As ARR_TIME,
       (COUNT(*)-COUNT(ARR_DELAY)) / COUNT(*) As ARR_DELAY,
       (COUNT(*)-COUNT(ARR_DELAY_NEW)) / COUNT(*) As ARR_DELAY_NEW,
       (COUNT(*)-COUNT(ARR_DEL15)) / COUNT(*) As ARR_DEL15,
       (COUNT(*)-COUNT(ARR_DELAY_GROUP)) / COUNT(*) As ARR_DELAY_GROUP,
       (COUNT(*)-COUNT(ARR_TIME_BLK)) / COUNT(*) As ARR_TIME_BLK,
       (COUNT(*)-COUNT(CANCELLED)) / COUNT(*) As CANCELLED,
       (COUNT(*)-COUNT(CANCELLATION_CODE)) / COUNT(*) As CANCELLATION_CODE,
       (COUNT(*)-COUNT(DIVERTED)) / COUNT(*) As DIVERTED,
       (COUNT(*)-COUNT(CRS_ELAPSED_TIME)) / COUNT(*) As CRS_ELAPSED_TIME,
       (COUNT(*)-COUNT(ACTUAL_ELAPSED_TIME)) / COUNT(*) As ACTUAL_ELAPSED_TIME,
       (COUNT(*)-COUNT(AIR_TIME)) / COUNT(*) As AIR_TIME,
       (COUNT(*)-COUNT(FLIGHTS)) / COUNT(*) As FLIGHTS,
       (COUNT(*)-COUNT(DISTANCE)) / COUNT(*) As DISTANCE,
       (COUNT(*)-COUNT(DISTANCE_GROUP)) / COUNT(*) As DISTANCE_GROUP,
       (COUNT(*)-COUNT(CARRIER_DELAY)) / COUNT(*) As CARRIER_DELAY,
       (COUNT(*)-COUNT(WEATHER_DELAY)) / COUNT(*) As WEATHER_DELAY,
       (COUNT(*)-COUNT(NAS_DELAY)) / COUNT(*) As NAS_DELAY,
       (COUNT(*)-COUNT(SECURITY_DELAY)) / COUNT(*) As SECURITY_DELAY,
       (COUNT(*)-COUNT(LATE_AIRCRAFT_DELAY)) / COUNT(*) As LATE_AIRCRAFT_DELAY,
       (COUNT(*)-COUNT(FIRST_DEP_TIME)) / COUNT(*) As FIRST_DEP_TIME,
       (COUNT(*)-COUNT(TOTAL_ADD_GTIME)) / COUNT(*) As TOTAL_ADD_GTIME,
       (COUNT(*)-COUNT(LONGEST_ADD_GTIME)) / COUNT(*) As LONGEST_ADD_GTIME,
       (COUNT(*)-COUNT(DIV_AIRPORT_LANDINGS)) / COUNT(*) As DIV_AIRPORT_LANDINGS,
       (COUNT(*)-COUNT(DIV_REACHED_DEST)) / COUNT(*) As DIV_REACHED_DEST,
       (COUNT(*)-COUNT(DIV_ACTUAL_ELAPSED_TIME)) / COUNT(*) As DIV_ACTUAL_ELAPSED_TIME,
       (COUNT(*)-COUNT(DIV_ARR_DELAY)) / COUNT(*) As DIV_ARR_DELAY,
       (COUNT(*)-COUNT(DIV_DISTANCE)) / COUNT(*) As DIV_DISTANCE,
       (COUNT(*)-COUNT(DIV1_AIRPORT)) / COUNT(*) As DIV1_AIRPORT,
       (COUNT(*)-COUNT(DIV1_AIRPORT_ID)) / COUNT(*) As DIV1_AIRPORT_ID,
       (COUNT(*)-COUNT(DIV1_AIRPORT_SEQ_ID)) / COUNT(*) As DIV1_AIRPORT_SEQ_ID,
       (COUNT(*)-COUNT(DIV1_WHEELS_ON)) / COUNT(*) As DIV1_WHEELS_ON,
       (COUNT(*)-COUNT(DIV1_TOTAL_GTIME)) / COUNT(*) As DIV1_TOTAL_GTIME,
       (COUNT(*)-COUNT(DIV1_LONGEST_GTIME)) / COUNT(*) As DIV1_LONGEST_GTIME,
       (COUNT(*)-COUNT(DIV1_WHEELS_OFF)) / COUNT(*) As DIV1_WHEELS_OFF,
       (COUNT(*)-COUNT(DIV1_TAIL_NUM)) / COUNT(*) As DIV1_TAIL_NUM,
       (COUNT(*)-COUNT(DIV2_AIRPORT)) / COUNT(*) As DIV2_AIRPORT,
       (COUNT(*)-COUNT(DIV2_AIRPORT_ID)) / COUNT(*) As DIV2_AIRPORT_ID,
       (COUNT(*)-COUNT(DIV2_AIRPORT_SEQ_ID)) / COUNT(*) As DIV2_AIRPORT_SEQ_ID,
       (COUNT(*)-COUNT(DIV2_WHEELS_ON)) / COUNT(*) As DIV2_WHEELS_ON,
       (COUNT(*)-COUNT(DIV2_TOTAL_GTIME)) / COUNT(*) As DIV2_TOTAL_GTIME,
       (COUNT(*)-COUNT(DIV2_LONGEST_GTIME)) / COUNT(*) As DIV2_LONGEST_GTIME,
       (COUNT(*)-COUNT(DIV2_WHEELS_OFF)) / COUNT(*) As DIV2_WHEELS_OFF,
       (COUNT(*)-COUNT(DIV2_TAIL_NUM)) / COUNT(*) As DIV2_TAIL_NUM,
       (COUNT(*)-COUNT(DIV3_AIRPORT)) / COUNT(*) As DIV3_AIRPORT,
       (COUNT(*)-COUNT(DIV3_AIRPORT_ID)) / COUNT(*) As DIV3_AIRPORT_ID,
       (COUNT(*)-COUNT(DIV3_AIRPORT_SEQ_ID)) / COUNT(*) As DIV3_AIRPORT_SEQ_ID,
       (COUNT(*)-COUNT(DIV3_WHEELS_ON)) / COUNT(*) As DIV3_WHEELS_ON,
       (COUNT(*)-COUNT(DIV3_TOTAL_GTIME)) / COUNT(*) As DIV3_TOTAL_GTIME,
       (COUNT(*)-COUNT(DIV3_LONGEST_GTIME)) / COUNT(*) As DIV3_LONGEST_GTIME,
       (COUNT(*)-COUNT(DIV3_WHEELS_OFF)) / COUNT(*) As DIV3_WHEELS_OFF,
       (COUNT(*)-COUNT(DIV3_TAIL_NUM)) / COUNT(*) As DIV3_TAIL_NUM,
       (COUNT(*)-COUNT(DIV4_AIRPORT)) / COUNT(*) As DIV4_AIRPORT,
       (COUNT(*)-COUNT(DIV4_AIRPORT_ID)) / COUNT(*) As DIV4_AIRPORT_ID,
       (COUNT(*)-COUNT(DIV4_AIRPORT_SEQ_ID)) / COUNT(*) As DIV4_AIRPORT_SEQ_ID,
       (COUNT(*)-COUNT(DIV4_WHEELS_ON)) / COUNT(*) As DIV4_WHEELS_ON,
       (COUNT(*)-COUNT(DIV4_TOTAL_GTIME)) / COUNT(*) As DIV4_TOTAL_GTIME,
       (COUNT(*)-COUNT(DIV4_LONGEST_GTIME)) / COUNT(*) As DIV4_LONGEST_GTIME,
       (COUNT(*)-COUNT(DIV4_WHEELS_OFF)) / COUNT(*) As DIV4_WHEELS_OFF,
       (COUNT(*)-COUNT(DIV4_TAIL_NUM)) / COUNT(*) As DIV4_TAIL_NUM,
       (COUNT(*)-COUNT(DIV5_AIRPORT)) / COUNT(*) As DIV5_AIRPORT,
       (COUNT(*)-COUNT(DIV5_AIRPORT_ID)) / COUNT(*) As DIV5_AIRPORT_ID,
       (COUNT(*)-COUNT(DIV5_AIRPORT_SEQ_ID)) / COUNT(*) As DIV5_AIRPORT_SEQ_ID,
       (COUNT(*)-COUNT(DIV5_WHEELS_ON)) / COUNT(*) As DIV5_WHEELS_ON,
       (COUNT(*)-COUNT(DIV5_TOTAL_GTIME)) / COUNT(*) As DIV5_TOTAL_GTIME,
       (COUNT(*)-COUNT(DIV5_LONGEST_GTIME)) / COUNT(*) As DIV5_LONGEST_GTIME,
       (COUNT(*)-COUNT(DIV5_WHEELS_OFF)) / COUNT(*) As DIV5_WHEELS_OFF,
       (COUNT(*)-COUNT(DIV5_TAIL_NUM)) / COUNT(*) As DIV5_TAIL_NUM
FROM bronze_air_traffic""" )
display(null_val_counts)

# COMMAND ----------

# Collect Rows, index the first (contains null value counts), and store as dictionary
null_val_pcts_dict = null_val_counts.collect()[0].asDict()
null_val_pcts_dict

# COMMAND ----------

# Get names of columns we are keeping
thresh = 0.20
nullCols20 = [] # columns with > 20% null values
to_keep = []
for c in df.columns:
  if null_val_pcts_dict[c] > thresh:
    nullCols20.append(c)
  else:
    to_keep.append(c)
    
print("Num to drop:", len(nullCols20), "\nNum to Keep", len(to_keep), "\nCol Names:", nullCols20)

# COMMAND ----------

# Drop columns above threshold of null values and write to table
printShape(df)
df_dropNulls20 = df.select(to_keep)
printShape(df_dropNulls20)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE dscc202_group02_db

# COMMAND ----------

# Overwrite table
try:
  df_dropNulls20.write.mode("overwrite").saveAsTable("dscc202_group02_db.bronze_air_traffic_cleaned_v1")
except:
  print("Dropping and updating table.")
  spark.sql("DROP TABLE IF EXISTS dscc202_group02_db.bronze_air_traffic_cleaned_v1")
  df_dropNulls20.write.mode("overwrite").saveAsTable("dscc202_group02_db.bronze_air_traffic_cleaned_v1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dscc202_group02_db.bronze_air_traffic_cleaned_v1 LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Logically Useless data

# COMMAND ----------

# Ryan - Load data
logicalCleanDF = spark.sql("SELECT * FROM bronze_air_traffic_cleaned_v1")

# COMMAND ----------

# Ryan - drop columns unneeded.

# DEST_AIRPORT_ID, DEST_AIRPORT_SEQ_ID, DEST_CITY_MARKET_ID, DEST, DEST_CITY_NAME, 
# DEST_STATE_ABR, DEST_STATE_FIPS, DEST_STATE_NM, DEST_WAC all contain very overlapping data. 
# I suggest we keep only DEST, as the others are redundant.
logicalCleanDF = logicalCleanDF.drop("DEST_AIRPORT_ID", "DEST_AIRPORT_SEQ_ID", 
                                     "DEST_CITY_MARKET_ID", "DEST_CITY_NAME", 
                                     "DEST_STATE_ABR", "DEST_STATE_FIPS", 
                                     "DEST_STATE_NM", "DEST_WAC");

# Similarly for origin:
logicalCleanDF = logicalCleanDF.drop("ORIGIN_AIRPORT_ID", "ORIGIN_AIRPORT_SEQ_ID", 
                                     "ORIGIN_CITY_MARKET_ID", "ORIGIN_CITY_NAME", 
                                     "ORIGIN_STATE_ABR", "ORIGIN_STATE_FIPS", 
                                     "ORIGIN_STATE_NM", "ORIGIN_WAC")

# It looks like fields DEP_DELAY_NEW and ARR_DELAY_NEW min at 0, 
# whereas the non-new ones can be negative. We probably want that negative data.
logicalCleanDF = logicalCleanDF.drop("DEP_DELAY_NEW", "ARR_DELAY_NEW");

#If we are keeping month, drop quarter
logicalCleanDF = logicalCleanDF.drop("QUARTER", "ARR_DELAY_NEW");

#OP_UNIQUE_CARRIER, OP_CARRIER_AIRLINE_ID, OP_CARRIER are all very similar only keep one. 
# I think OP_UNIQUE_CARRIER is the best choice
logicalCleanDF = logicalCleanDF.drop("OP_CARRIER_AIRLINE_ID", "OP_CARRIER");

#Flights is 1 for every row.
logicalCleanDF = logicalCleanDF.drop("FLIGHTS");

# display(logicalCleanDF.select(countDistinct("DIVERTED")))
# logicalCleanDF.groupBy('DIVERTED').count().show()

# COMMAND ----------

# Refining DF by dropping more columns -- Nishith

# The diagram professor gave us in the main notebook says we only need to use flight number 
# to show estimate so following fields seems irrelevant for our analysis
logicalCleanDF = logicalCleanDF.drop("TAIL_NUM");
logicalCleanDF = logicalCleanDF.drop("OP_UNIQUE_CARRIER");

#CRS_DEP_TIME and CRS_ARR_TIME is the actual scheduled flight departure and arrival we need,
# so renamed it to make it more understandable.
logicalCleanDF = logicalCleanDF.withColumnRenamed("CRS_DEP_TIME", "SCHEDULED_DEP_TIME")
logicalCleanDF = logicalCleanDF.withColumnRenamed("CRS_ARR_TIME", "SCHEDULED_ARR_TIME")

# these field flag if delay/arrival is or more than 15 mins. 
# this doesn't seem necessary given we have DEP_DELAY and ARR_DELAY
logicalCleanDF = logicalCleanDF.drop("DEP_DEL15");
logicalCleanDF = logicalCleanDF.drop("ARR_DEL15");

# I believe these fields are just grouping each row based on delay time, 
# arrival time and flight distance. It doesn't seem relevant for our analysis.
logicalCleanDF = logicalCleanDF.drop("DEP_DELAY_GROUP");
logicalCleanDF = logicalCleanDF.drop("ARR_DELAY_GROUP");
logicalCleanDF = logicalCleanDF.drop("DISTANCE_GROUP");

printShape(logicalCleanDF)
display(logicalCleanDF);

# COMMAND ----------

# Overwrite table
try:
  logicalCleanDF.write.mode("overwrite").saveAsTable("dscc202_group02_db.bronze_air_traffic_cleaned_v2")
except:
  print("Dropping and Updating table.")
  spark.sql("DROP TABLE IF EXISTS dscc202_group02_db.bronze_air_traffic_cleaned_v2")
  logicalCleanDF.write.mode("overwrite").saveAsTable("dscc202_group02_db.bronze_air_traffic_cleaned_v2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *  FROM dscc202_group02_db.bronze_air_traffic_cleaned_v2 LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast Time columns to TimestampType

# COMMAND ----------

airportsOfInterestDF = spark.sql("""
  SELECT * 
  FROM dscc202_group02_db.bronze_air_traffic_cleaned_v2 
  WHERE ORIGIN IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
  OR DEST IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
 """)
# printShape(airportsOfInterestDF)

print(airportsOfInterestDF.count(), len(airportsOfInterestDF.columns))

# COMMAND ----------

display(airportsOfInterestDF.select([count(when(isnull(c), c)).alias(c) for c in airportsOfInterestDF.columns]))

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *

airportsOfInterestDF = ( airportsOfInterestDF
                        # SCHEDULED_DEP_TIME concatenated with FL_DATE casted to TimestampType
                        .withColumn('SCHEDULED_DEP_TIME', to_timestamp(concat(airportsOfInterestDF.FL_DATE.cast('string'),
                                                                               lit(' '),
                                                                               lpad(airportsOfInterestDF.SCHEDULED_DEP_TIME.cast('string'), 4, '0')), 'yyyy-MM-dd HHmm'))
                        # SCHEDULE_ARR_TIME concatenated with FL_DATE casted to TimestampType
                        .withColumn('SCHEDULED_ARR_TIME', to_timestamp(concat(airportsOfInterestDF.FL_DATE.cast('string'),
                                                                               lit(' '),
                                                                               lpad(airportsOfInterestDF.SCHEDULED_ARR_TIME.cast('string'), 4, '0')), 'yyyy-MM-dd HHmm'))
                        # WHEELS_OFF concatenated with FL_DATE casted to TimestampType
                        .withColumn('WHEELS_OFF', to_timestamp(concat(airportsOfInterestDF.FL_DATE.cast('string'),
                                                                       lit(' '),
                                                                       lpad(airportsOfInterestDF.WHEELS_OFF.cast('string'), 4, '0')), 'yyyy-MM-dd HHmm'))
                        # WHEELS_ON concatenated with FL_DATE casted to TimestampType
                        .withColumn('WHEELS_ON', to_timestamp(concat(airportsOfInterestDF.FL_DATE.cast('string'),
                                                                       lit(' '),
                                                                       lpad(airportsOfInterestDF.WHEELS_ON.cast('string'), 4, '0')), 'yyyy-MM-dd HHmm'))
                        # Dropping Redundant Columns
                        .drop("YEAR", "MONTH", "DAY_OF_MONTH", "FL_DATE", "DEP_TIME", "DEP_TIME_BLK", "ARR_TIME", "ARR_TIME_BLK") ).na.drop()
printShape(airportsOfInterestDF)
display(airportsOfInterestDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE dscc202_group02_db

# COMMAND ----------

# Overwrite table
try:
  airportsOfInterestDF.write.mode("overwrite").saveAsTable("dscc202_group02_db.bronze_air_traffic_cleaned_v3")
except:
  print("Dropping and updating table.")
  spark.sql("DROP TABLE IF EXISTS dscc202_group02_db.bronze_air_traffic_cleaned_v3")
  airportsOfInterestDF.write.mode("overwrite").saveAsTable("dscc202_group02_db.bronze_air_traffic_cleaned_v3")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dscc202_group02_db.bronze_air_traffic_cleaned_v3 LIMIT 10

# COMMAND ----------

# TODO: fix dataset so that flights going into the next day use FL_DATE + 1 for the yyyy-MM-dd portion of the arrival time

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Cleaning Weather Data

# COMMAND ----------

from pyspark.sql.functions import *
weather_airports = spark.sql("""SELECT * FROM dscc202_group02_db.bronze_airport_weather_join""")
display(weather_airports.select([count(when(isnull(c), c)).alias(c) for c in weather_airports.columns]))
print(weather_airports.count())

# COMMAND ----------

from sklearn.impute import KNNImputer
import numpy as np

def impute(df, groupby, targets):
  weather_averages = df.na.drop().groupBy(*groupby).agg(
    *[avg(t).alias("grouped_" + t) for t in targets]
  )
  df_with_avg = df.join(weather_averages, groupby, "left")

  prev_imputation_df = df_with_avg;
  for t in targets:
    prev_imputation_df = prev_imputation_df.withColumn("imputed_"+t, when(col(t).isNull(), col("grouped_" + t)).otherwise(col(t)))
  imputed_weather = prev_imputation_df.drop(*["grouped_" + t for t in targets]).drop(*[t for t in targets])
  for t in targets:
    imputed_weather = imputed_weather.withColumnRenamed("imputed_"+t, t)
  return imputed_weather




orgin_imputation_targets = ["orgin_tot_precip_mm",
                      "orgin_avg_wnd_mps", 
                      "orgin_avg_vis_m",
                      "orgin_avg_slp_hpa",
                      "orgin_avg_dewpt_f",
                      ];
dest_imputation_targets = ["dest_tot_precip_mm",
                          "dest_avg_wnd_mps",
                          "dest_avg_vis_m",
                          "dest_avg_slp_hpa",
                          "dest_avg_dewpt_f"];

weather_airports = weather_airports.withColumn("MONTH_OF_YEAR", month("SCHEDULED_DEP_TIME"))


orgin_impute = impute(weather_airports, ["ORIGIN", "MONTH_OF_YEAR"], orgin_imputation_targets)
full_impute = impute(orgin_impute, ["DEST", "MONTH_OF_YEAR"], dest_imputation_targets)
full_impute = full_impute.drop("MONTH_OF_YEAR")
display(full_impute)
display(full_impute.select([count(when(isnull(c), c)).alias(c) for c in full_impute.columns]))

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.utils import AnalysisException
# overwrite table.
try:
  full_impute.write.mode("overwrite").saveAsTable("dscc202_group02_db.bronze_airport_weather_join_imputed")
except:
  print("Dropping and updating table.")
  spark.sql("DROP TABLE IF EXISTS dscc202_group02_db.bronze_airport_weather_join_imputed")
  full_impute.write.mode("overwrite").saveAsTable("dscc202_group02_db.bronze_airport_weather_join_imputed")




# COMMAND ----------

display(airportDF.select(countDistinct("ORIGIN")))