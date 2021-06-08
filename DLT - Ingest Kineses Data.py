# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


my_stream_name = 'turbine' 
kinesisRegion = 'us-west-2'

@dlt.create_table(
  name='iot_bronze_kinesis',
  comment="BRONZE TABLE FOR IOT WIND TURBINE DATA FROM KINESIS",
  path="/Users/volker.tjaden@databricks.com/dev/iot_bronze_kinesis",
)
def kinesis_stream():
    return (
      spark
      .readStream
      .format("kinesis")
      .option("streamName", my_stream_name)
      .option("initialPosition", "latest")
      .option("region", kinesisRegion)
      .load()
      .withColumn('value',col("data").cast(StringType()))                
      .withColumn('key',col('partitionKey').cast(DoubleType()))
      .select('key','value')
    )

# COMMAND ----------

jsonSchema = StructType([StructField(col, DoubleType(), False) for col in ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10", "SPEED", "TORQUE", "ID"]] + [StructField("TIMESTAMP", TimestampType())])
@dlt.create_table(
  comment="Turbine dataset with cleaned-up datatypes / column names and quality expectations.",
  table_properties={
    "quality": "silver"
  }
)
@dlt.expect("TIMESTAMP Constraint", "TIMESTAMP IS NOT NULL")
@dlt.expect("SPEED constraint", "SPEED >= 0")
@dlt.expect("ID", "ID > 0")
def turbine_clean():
  return (
    read_stream("iot_bronze_kinesis")
      .withColumn("jsonData", from_json(col("value"), jsonSchema)).select("jsonData.*") 
  )

# COMMAND ----------

hostname = dbutils.secrets.get( "oetrta", "redshift-hostname" )
port     = dbutils.secrets.get( "oetrta", "redshift-port"     )
database = dbutils.secrets.get( "oetrta", "redshift-database" )
username = dbutils.secrets.get( "oetrta", "redshift-username" )
password = dbutils.secrets.get( "oetrta", "redshift-password" )
tempdir  = dbutils.secrets.get( "oetrta", "redshift-temp-dir" )
iam_role = dbutils.secrets.get( "oetrta", "redshift-iam-role" )
redshift_url = f"jdbc:postgresql://{hostname}:{port}/{database}?user={username}&password={password}"
  
@dlt.create_table(
  comment="A table of the device status",
  table_properties={
    "quality": "gold"
  }
)
@dlt.expect_or_fail("status", "status is not null")
def device_status():
  return (spark.read 
            .format( "jdbc") 
            .option("driver","org.postgresql.Driver")
            .option( "url",          redshift_url   ) 
            .option( "dbtable",      "turbine_status") 
            .option( "tempdir",      tempdir        ) 
            .option( "aws_iam_role", iam_role       )
            .load()
          )

# COMMAND ----------

@dlt.create_table(
  comment="The list of devices with their status",
  table_properties={
    "quality": "gold"
  }
 )
def turbine_gold():
  return  read_stream("turbine_clean").join(read("device_status"), ['id'], 'left')
