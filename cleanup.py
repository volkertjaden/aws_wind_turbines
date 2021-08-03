# Databricks notebook source
dbname = "volker_windturbine"
path = "s3://oetrta/volker/demos/turbine/"

# COMMAND ----------

spark.sql(f'DROP DATABASE {dbname} CASCADE')

# COMMAND ----------

dbutils.fs.rm(path,True)
