# Databricks notebook source
# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC USE dscc202_db

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_air_traffic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Low Quality Columns

# COMMAND ----------

# Alex Crystal - Load Bronze Data into spark DataFrame
df = spark.sql("SELECT * FROM bronze_air_traffic")

# COMMAND ----------

# Alex Crystal - Print the SQL query for the next code cell
print("SELECT")
for c in df.columns:
  print(f"       COUNT(*)-COUNT({c}) As {c}", end='')
  if c!=df.columns[-1]:
    print(",")
  else:
    print("\nFROM bronze_air_traffic")

# COMMAND ----------

# Alex Crystal - Get the number of null values in each column
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

# MAGIC %md
# MAGIC ##### Alex Crystal - Collect values and make dictionary of column names mapped to num null values.

# COMMAND ----------

# Collect Rows, index the first (contains null value counts), and store as dictionary
null_val_counts_dict = null_value_counts.collect()[0].asDict()
null_val_counts_dict

# COMMAND ----------

# Alex Crystal - Create null value percentage dictionary
nrows, ncols = df.count(), len(df.columns)
null_val_pcts = {col_name:nvc/nrows for col_name, nvc in null_value_counts_dict.items()}
print("Rows:",nrows,"\nColumns:",ncols,"\nNull Value Percentages:",null_val_pcts)

# COMMAND ----------

# Alex Crystal - Get names of columns we are keeping
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

# Alex Crystal - Drop columns above threshold of null values and write to table
df_dropNulls20 = df.select(to_keep)
df_dropNulls20.write.saveAsTable("bronze_air_traffic_dropNulls20")

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Alex Crystal - Query table to assert that it was written successfully */
# MAGIC SELECT * FROM bronze_air_traffic_dropNulls20

# COMMAND ----------

