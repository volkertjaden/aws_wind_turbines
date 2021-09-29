# Databricks notebook source
dbname = "volker_windturbine"
path = "s3://oetrta/volker/demos/turbine/"

my_stream_name = 'turbine' 
kinesisRegion = 'us-west-2'

# COMMAND ----------

spark.sql(f'DROP DATABASE {dbname} CASCADE')

# COMMAND ----------

dbutils.fs.rm(path,True)

# COMMAND ----------

import boto3
import json
kinesis_client = boto3.client('kinesis', region_name=kinesisRegion)

response = kinesis_client.delete_stream(
    StreamName=my_stream_name
)

print(response)
