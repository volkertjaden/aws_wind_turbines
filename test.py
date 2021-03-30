# Databricks notebook source
# MAGIC %fs ls /mnt/oetrta/volker/turbine/

# COMMAND ----------

df = spark.read.parquet('/mnt/oetrta/volker/turbine/incoming-data')
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %sql show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC use volker_tjaden_databricks_com

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from turbine_power
