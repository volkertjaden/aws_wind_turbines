# Databricks notebook source
# MAGIC %run ./00-setup_power $reset_all="False" $path="s3://oetrta/volker/demos" $dbName="volker_windturbine"

# COMMAND ----------

from pyspark.sql.functions import to_json, current_timestamp, struct

import boto3
import json
from time import sleep

kinesis_client = boto3.client('kinesis', region_name='us-west-2')

# COMMAND ----------

jsonSchema = StructType([StructField(col, DoubleType(), False) for col in ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10", "SPEED", "ID"]] + [StructField("TIMESTAMP", TimestampType())])

df = (spark.read.format('delta').load("s3://oetrta/volker/datasets/turbine/kinesis_sample/")
     .withColumn("jsonData", from_json(col("value"), jsonSchema)) \
     .select("key","jsonData.*")
     )

# COMMAND ----------

df.cache()

# COMMAND ----------

df_sample = (df
             .sample(fraction=0.01)
             .limit(500)
             .dropDuplicates(["ID"])
             .withColumn("TIMESTAMP",current_timestamp())
             .select('key',to_json(struct(col('*'))).alias('value'))
            )

pdf = df_sample.toPandas()

display(df_sample)

# COMMAND ----------

while True:
  df_sample = (df
             .sample(fraction=0.01)
             .limit(500)
             .dropDuplicates(["ID"])
             .withColumn("TIMESTAMP",current_timestamp())
             .select('key',to_json(struct(col('*'))).alias('value'))
            )

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
