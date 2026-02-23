#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


# In[ ]:


credentials_location = '/Users/mac/Desktop/zoomcamp2026/lydia-zoomcamp-b3149c74b751.json'
jar_path = '/Users/mac/Desktop/02-workflow-orchestration/06-batch/lib/gcs-connector-hadoop3-2.2.5.jar'
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", './lib/gcs-connector-hadoop3-2.2.5.jar') \
    .set("spark.driver.extraClassPath", jar_path) \
    .set("spark.executor.extraClassPath", jar_path) \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)



sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
# Add these to your existing hadoop_conf.set list
hadoop_conf.set("google.cloud.auth.service.account.enable", "true") # Sometimes both are needed
hadoop_conf.set("fs.gs.glob.flatlist", "true") # Helps with wildcard expansion in GCS

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()



df_yellow = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet('gs://lydia-zoomcamp-kestra-bucket/pq/yellow/2025/')


df_yellow.count()


print("Starting the count...")
count = df_yellow.count()
print(f"Total rows in 2025 Yellow Taxi data: {count}")

# Always stop the session at the end of a script
spark.stop()



