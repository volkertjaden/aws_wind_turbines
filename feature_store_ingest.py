# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS volker_windturbine.gold_ml
USING delta
LOCATION '/mnt/oetrta/volker/turbine/gold_ml/'
""")

# COMMAND ----------

from pyspark.sql.functions import *
from databricks import feature_store

# COMMAND ----------

df = spark.read.table('volker_windturbine.gold_ml')

# COMMAND ----------

display(df)

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

df = df.distinct()

# COMMAND ----------

df.cache()

# COMMAND ----------

display(df.groupby('id','timestamp','status').count().orderBy(desc('count')))

# COMMAND ----------

df = df.withColumn('uid',monotonically_increasing_id())

# COMMAND ----------

fs.create_feature_table(
    name="volker_windturbine.feature_table",
    keys=["uid"],
    features_df=df,
    description="Sensor readings from wind turbine with turbine state",
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

help(fs.create_feature_table)
