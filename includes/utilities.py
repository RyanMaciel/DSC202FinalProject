# Databricks notebook source
"""
## Helper routines...
- mounting buckets in s3
- use of database
- creation of tables in the metastore
- ploting
- data reading
"""

import mlflow
import pandas as pd
import tempfile
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

class Utils:
    @staticmethod
    def mount_datasets(group_name):
      class_mount_name = "dscc202-datasets"
      group_mount_name = "dscc202-{}-datasets".format(group_name)
      
      class_s3_bucket = "s3a://dscc202-datasets/"
      try:
        dbutils.fs.mount(class_s3_bucket, "/mnt/%s" % class_mount_name)
      except:
        dbutils.fs.unmount("/mnt/%s" % class_mount_name)
        dbutils.fs.mount(class_s3_bucket, "/mnt/%s" % class_mount_name)
        
      group_s3_bucket = "s3a://dscc202-{}-datasets/".format(group_name)
      try:
        dbutils.fs.mount(group_s3_bucket, "/mnt/%s" % group_mount_name)
      except:
        dbutils.fs.unmount("/mnt/%s" % group_mount_name)
        dbutils.fs.mount(group_s3_bucket, "/mnt/%s" % group_mount_name)
        
      return "/mnt/%s" % class_mount_name, "/mnt/%s" % group_mount_name

    @staticmethod
    def create_metastore(group_name):
      # Setup the hive meta store if it does not exist and select database as the focus of future sql commands in this notebook
      spark.sql(f"CREATE DATABASE IF NOT EXISTS dscc202_{group_name}_db")
      spark.sql(f"USE dscc202_{group_name}_db")
      return f"dscc202_{group_name}_db"
      
    @staticmethod
    def create_delta_dir(group_name):
      delta_dir = f"/mnt/dscc202-{group_name}-datasets/flightdelay/tables/"
      dbutils.fs.mkdirs(delta_dir)
      return delta_dir