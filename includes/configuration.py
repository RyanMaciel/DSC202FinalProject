# Databricks notebook source
## Enter your group specific information here...

GROUP='group02'   # CHANGE TO YOUR GROUP NAME

# COMMAND ----------

"""
Enter any project wide configuration here...
- paths
- table names
- constants
- etc
"""

# Some configuration of the cluster
spark.conf.set("spark.sql.shuffle.partitions", "32")  # Configure the size of shuffles the same as core count on your cluster
spark.conf.set("spark.sql.adaptive.enabled", "true")  # Spark 3.0 AQE - coalescing post-shuffle partitions, converting sort-merge join to broadcast join, and skew join optimization

# Mount the S3 class and group specific buckets
CLASS_DATA_PATH, GROUP_DATA_PATH = Utils.mount_datasets(GROUP)

# create the delta tables base dir in the s3 bucket for your group
BASE_DELTA_PATH = Utils.create_delta_dir(GROUP)

# Create the metastore for your group and set as the default db
GROUP_DBNAME = Utils.create_metastore(GROUP)

# class DB Name (bronze data resides here)
CLASS_DBNAME = "dscc202_db"

# COMMAND ----------

displayHTML(f"""
<table border=1>
<tr><td><b>Variable Name</b></td><td><b>Value</b></td></tr>
<tr><td>CLASS_DATA_PATH</td><td>{CLASS_DATA_PATH}</td></tr>
<tr><td>GROUP_DATA_PATH</td><td>{GROUP_DATA_PATH}</td></tr>
<tr><td>BASE_DELTA_PATH</td><td>{BASE_DELTA_PATH}</td></tr>
<tr><td>GROUP_DBNAME</td><td>{GROUP_DBNAME}</td></tr>
<tr><td>CLASS_DBNAME</td><td>{CLASS_DBNAME}</td></tr>
</table>
""")
