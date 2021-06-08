# Databricks notebook source
# MAGIC %run ./00-setup_power $reset_all="False"

# COMMAND ----------

import boto3
import json
from time import sleep

kinesis_client = boto3.client('kinesis', region_name='us-west-2')

# COMMAND ----------

df = spark.read.format('delta').load(path+"/turbine/kinesis_sample/")

# COMMAND ----------

while True:
  df_sample = df.sample(fraction=0.01).limit(500)
  pdf = df_sample.toPandas()

  Records = []
  for index, row in pdf.iterrows():
      Records.append(
      {
        'Data': row['value'],
        'PartitionKey': str(int(row['key']))
      })

  response = kinesis_client.put_records(
      Records=Records,
      StreamName=my_stream_name
  )
  
  sleep(5)

# COMMAND ----------


