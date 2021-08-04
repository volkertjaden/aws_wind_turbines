# Databricks notebook source
# MAGIC %fs ls /mnt/oetrta/volker/turbine/

# COMMAND ----------

dbutils.fs.rm('/home/volker.tjaden@databricks.com/turbine/turbine',True)

# COMMAND ----------

# MAGIC %fs ls /home/volker.tjaden@databricks.com/turbine

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from volker_windturbine.turbine_bronze_historical

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from volker_windturbine.turbine_bronze_kinesis

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from volker_windturbine.turbine_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from volker_windturbine.turbine_gold

# COMMAND ----------

df2 = spark.read.parquet('/mnt/oetrta/volker/turbine/incoming-data/')

# COMMAND ----------

df2.count()

# COMMAND ----------

spark.read.parquet("/Users/volker.tjaden@databricks.com/dev/wind_sample").count()

# COMMAND ----------

df = spark.read.parquet('/mnt/oetrta/volker/turbine/incoming-data')
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC use volker_dev;
# MAGIC show tables;

# COMMAND ----------

df_bronze = spark.read.load('/Users/volker.tjaden@databricks.com/dev/iot_bronze')

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

df_bronze.createTempView('iot_bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   json.*
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       from_json(value, 'AN3 double, AN4 double, AN5 double, AN6 double, AN7 double, AN8 double, AN9  double, AN10 double, SPEED double, TORQUE double, ID double, TIMESTAMP timestamp') as json
# MAGIC     from
# MAGIC       iot_bronze
# MAGIC   )
# MAGIC limit
# MAGIC   10

# COMMAND ----------

df_bronze.count()

# COMMAND ----------

dbutils.fs.ls('/Users/volker.tjaden@databricks.com/dev/iot_bronze')

# COMMAND ----------

dbutils.fs.ls('/Users/volker.tjaden@databricks.com/dev/pipelines/windturbine/system/events')
