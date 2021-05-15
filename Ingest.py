# Databricks notebook source
# MAGIC %sql
# MAGIC USE dscc202_db

# COMMAND ----------

spark.read.format('delta').table("dscc202_db.bronze_air_traffic").createOrReplaceTempView("streaming_airport_temp_view")
spark.read.format('delta').table("dscc202_db.bronze_weather").createOrReplaceTempView("streaming_weather_temp_view")
weatherDF = spark.sql("SELECT * FROM streaming_weather_temp_view")

# COMMAND ----------

from pyspark.sql.functions import *
display(weatherDF)
print(weatherDF.schema.names)

# COMMAND ----------

from pyspark.sql.functions import *


# Ryan - Taken from the data validation demo notebook. Split up the weather data into a more parsable format.
priorColumns = weatherDF.schema.names

# Keep these columns.
keep = ["LATITUDE", "LONGITUDE", "NAME", "STATION"]
for keepVal in keep:
  if(keepVal in priorColumns):
    priorColumns.remove(keepVal)

print(priorColumns[1])
parsedWeatherDF =(weatherDF
        .withColumn('temp_f', split(col('TMP'),",")[0]*9/50+32)
        .withColumn('temp_qual', split(col('TMP'),",")[1])
        .withColumn('wnd_deg', split(col('WND'),",")[0])
        .withColumn('wnd_1', split(col('WND'),",")[1])
        .withColumn('wnd_2', split(col('WND'),",")[2])
        .withColumn('wnd_mps', split(col('WND'),",")[3]/10)
        .withColumn('wnd_4', split(col('WND'),",")[4])
        .withColumn('vis_m', split(col('VIS'),",")[0])
        .withColumn('vis_1', split(col('VIS'),",")[1])
        .withColumn('vis_2', split(col('VIS'),",")[2])
        .withColumn('vis_3', split(col('VIS'),",")[3])
        .withColumn('dew_pt_f', split(col('DEW'),",")[0]*9/50+32)
        .withColumn('dew_1', split(col('DEW'),",")[1])
        .withColumn('slp_hpa', split(col('SLP'),",")[0]/10)
        .withColumn('slp_1', split(col('SLP'),",")[1])
        .withColumn('precip_hr_dur', split(col('AA1'),",")[0])
        .withColumn('precip_mm_intvl', split(col('AA1'),",")[1]/10)
        .withColumn('precip_cond', split(col('AA1'),",")[2])
        .withColumn('precip_qual', split(col('AA1'),",")[3])
        .withColumn('precip_mm', col('precip_mm_intvl')/col('precip_hr_dur'))
        .withColumn("time", date_trunc('hour', "DATE"))
                 )
parsedWeatherDF = parsedWeatherDF.drop(*priorColumns)

# COMMAND ----------

display(parsedWeatherDF)


# COMMAND ----------

# Ryan - limit distances of weather stations from interested airports.
import functools

# Verified locations [lat, long] of all airports we are interested in.
airportLocations = {
  "JFK": [40.639722, -73.778889],
  "SEA": [47.448889, -122.309444],
  "BOS": [42.363056,-71.006389],
  "ATL": [33.636667,-84.428056],
  "LAX": [33.9425,-118.408056],
  "SFO": [37.618889,-122.375],
  "DEN": [39.861667,-104.673056],
  "DFW": [32.896944,-97.038056],
  "ORD": [41.978611,-87.904722],
  "CVG": [39.048889,-84.667778],
  "CLT": [35.213889,-80.943056],
  "DCA": [38.852222,-77.037778],
  "IAH": [29.984444,-95.341389],
}

# 0.1 lat or long degrees is about 11km at equator (varies at different points N/S https://en.wikipedia.org/wiki/Decimal_degrees)
# So lets just pay attention to weather stations within 0.2 degrees of the airport position and aggregate them.
distance = 0.2
filterExpressions = []
newColumnExpressions = None;
for name, location in airportLocations.items():
  
  expression = (((col("LATITUDE") > location[0]-(distance/2))) & (col("LATITUDE") < location[0] + (distance/2)) & 
                       (col("LONGITUDE") > location[1]-(distance/2)) & (col("LONGITUDE") < location[1] + (distance/2)))

  if newColumnExpressions is None:
    newColumnExpressions = when(expression, name)
  else:
    newColumnExpressions = newColumnExpressions.when(expression, name)
  filterExpressions.append(expression)
  

filterExpression = functools.reduce(lambda a,b : a | b, filterExpressions)
localWeatherDF = parsedWeatherDF.filter(filterExpression)

newColumnExpressions.otherwise("Unknown")
localWeatherDF = localWeatherDF.withColumn("close_airport", newColumnExpressions)
display(localWeatherDF)


# COMMAND ----------

print(localWeatherDF.count())

# COMMAND ----------

display(localWeatherDF.select([count(when(isnull(c), c)).alias(c) for c in localWeatherDF.columns]))

# COMMAND ----------

display(localWeatherDF.filter("precip_mm is NULL").select(countDistinct("close_airport")))

# COMMAND ----------

aggDF = localWeatherDF.groupby(["close_airport", "time"]).agg(mean('temp_f').alias('avg_temp_f'),
       mean('wnd_mps').alias('avg_wnd_mps'),
       mean('vis_m').alias('avg_vis_m'),
       mean('slp_hpa').alias('avg_slp_hpa'),
       mean('dew_pt_f').alias('avg_dewpt_f')
       ).orderBy("time")
display(aggDF)
#sum('precip_mm').alias('tot_precip_mm'),

# COMMAND ----------

display(aggDF.select([count(when(isnull(c), c)).alias(c) for c in aggDF.columns]))
print(aggDF.count())

# COMMAND ----------

# %sql
# USE dscc202_group02_db

# COMMAND ----------

# from pyspark.sql.utils import AnalysisException
# try:
#   aggDF.write.saveAsTable("bronze_weather_data_ingest_agg")
# except AnalysisException:
#   print("Table already exists")




# COMMAND ----------

from pyspark.sql.functions import *
# aggDF = spark.sql("SELECT * FROM bronze_weather_data_ingest_agg")


airportDF = spark.sql("""SELECT * FROM streaming_airport_temp_view 
                         WHERE ORIGIN IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
                         AND DEST IN ("JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH")
                       """)

airportDF = (airportDF.withColumn("SCHEDULED_DEP_TIME", date_trunc('hour', "SCHEDULED_DEP_TIME"))
             .withColumn("SCHEDULED_ARR_TIME", date_trunc('hour', "SCHEDULED_ARR_TIME")))




# COMMAND ----------

display(airportDF)

# COMMAND ----------

originAgg = aggDF.select(col("close_airport"),
                         col("time"),
                         col("avg_wnd_mps").alias("orgin_avg_wnd_mps"),
                         col("avg_vis_m").alias("orgin_avg_vis_m"),
                         col("avg_slp_hpa").alias("orgin_avg_slp_hpa"),
                         col("avg_dewpt_f").alias("orgin_avg_dewpt_f")
                        )
destAgg = aggDF.select(col("close_airport"),
                         col("time"),
                         col("avg_wnd_mps").alias("dest_avg_wnd_mps"),
                         col("avg_vis_m").alias("dest_avg_vis_m"),
                         col("avg_slp_hpa").alias("dest_avg_slp_hpa"),
                         col("avg_dewpt_f").alias("dest_avg_dewpt_f")
                        )
display(originAgg)
display(destAgg)

# COMMAND ----------

originJoined = airportDF.join(originAgg, [(airportDF["ORIGIN"] == originAgg["close_airport"]) &
                              (airportDF["SCHEDULED_DEP_TIME"] == originAgg["time"])], "left").drop("close_airport", "time")
fullJoined = originJoined.join(destAgg, [(originJoined["DEST"] == destAgg["close_airport"]) &
                              (originJoined["SCHEDULED_ARR_TIME"] == destAgg["time"])], "left").drop("close_airport", "time")


# COMMAND ----------

display(fullJoined)

# COMMAND ----------

display(fullJoined.select(countDistinct("ORIGIN")))
display(fullJoined.select(countDistinct("DEST")))
print(fullJoined.count())

# COMMAND ----------

display(fullJoined.select([count(when(isnull(c), c)).alias(c) for c in fullJoined.columns]))

# COMMAND ----------

display(fullJoined.filter("orgin_avg_vis_m is NULL").orderBy("SCHEDULED_DEP_TIME"))

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
# overwrite table.
try:
  fullJoined.write.mode("overwrite").saveAsTable("bronze_airport_weather_join")
except:
  print("Dropping and updating table.")
  spark.sql("DROP TABLE IF EXISTS bronze_airport_weather_join")
  airportsOfInterestDF.write.mode("overwrite").saveAsTable("bronze_airport_weather_join")



# COMMAND ----------

