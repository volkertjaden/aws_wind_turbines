# Databricks notebook source
# MAGIC %md
# MAGIC DLT options:
# MAGIC - "target": "volker_dev"
# MAGIC - "development": true
# MAGIC - path to repo: /Repos/volker.tjaden@databricks.com/aws_wind_turbines/01c - Ingest DLT

# COMMAND ----------

#paths
raw_path = '/Users/volker.tjaden@databricks.com/dev/wind_sample'
storage_path = '/Users/volker.tjaden@databricks.com/dev/pipelines/windturbine'
bronze_path = '/Users/volker.tjaden@databricks.com/dev/iot_bronze'
silver_path = '/Users/volker.tjaden@databricks.com/dev/iot_silver'

all_paths = [bronze_path, silver_path]

#tables 
bronze_table = 'volker_dev.iot_silver'
silver_table = 'volker_dev.iot_bronze'

all_tables = [bronze_table, silver_table]

# COMMAND ----------

dbutils.fs.ls('/Users/volker.tjaden@databricks.com/dev/')

# COMMAND ----------

reset = True

if reset:
  for tbl in all_tables:
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
  
  for p in all_paths:
    dbutils.fs.rm(p,True)

# COMMAND ----------

# DBTITLE 1,Raw data
# MAGIC %sql
# MAGIC select count(*) from parquet.`/Users/volker.tjaden@databricks.com/dev/wind_sample`

# COMMAND ----------

# DBTITLE 1,Check status of bronze table
# MAGIC %sql
# MAGIC select count(*) from volker_dev.iot_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history volker_dev.iot_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from volker_dev.iot_bronze_kinesis

# COMMAND ----------

# DBTITLE 1,Add records to raw table
(spark
 .read
 .parquet('/mnt/oetrta/volker/turbine/incoming-data/')
 .sample(fraction=0.001)
 .write
 .mode('append')
 .parquet(raw_path)
)

# COMMAND ----------

# DBTITLE 1,Constraints on Silver tables - Generate constraints
# MAGIC %sql
# MAGIC select * 
# MAGIC from volker_dev.iot_bronze
# MAGIC where from_json(value,'ID Double').ID < 0
# MAGIC limit 10
# MAGIC -- no data in data set do in stream

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from volker_dev.iot_silver
