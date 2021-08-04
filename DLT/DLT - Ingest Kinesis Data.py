# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Wind Turbine Predictive Maintenance
# MAGIC 
# MAGIC In this example, we demonstrate anomaly detection for the purposes of finding damaged wind turbines. A damaged, single, inactive wind turbine costs energy utility companies thousands of dollars per day in losses.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/turbine/turbine_flow.png" />
# MAGIC 
# MAGIC 
# MAGIC <div style="float:right; margin: -10px 50px 0px 50px">
# MAGIC   <img src="https://s3.us-east-2.amazonaws.com/databricks-knowledge-repo-images/ML/wind_turbine/wind_small.png" width="400px" /><br/>
# MAGIC   *locations of the sensors*
# MAGIC </div>
# MAGIC Our dataset consists of vibration readings coming off sensors located in the gearboxes of wind turbines. 
# MAGIC 
# MAGIC We will use Gradient Boosted Tree Classification to predict which set of vibrations could be indicative of a failure.
# MAGIC 
# MAGIC One the model is trained, we'll use MFLow to track its performance and save it in the registry to deploy it in production
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *Data Source Acknowledgement: This Data Source Provided By NREL*
# MAGIC 
# MAGIC *https://www.nrel.gov/docs/fy12osti/54530.pdf*

# COMMAND ----------

# DBTITLE 1,Ingest Streaming Data from Kinesis into Bronze Delta table
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


my_stream_name = 'turbine' 
kinesisRegion = 'us-west-2'

@dlt.create_table(
  name='turbine_bronze_kinesis',
  comment="BRONZE TABLE FOR IOT WIND TURBINE DATA FROM KINESIS",
  table_properties={
    "quality": "bronze"
  }
)
def kinesis_stream():
    return (
      spark
      .readStream
      .format("kinesis")
      .option("streamName", my_stream_name)
      .option("initialPosition", "earliest")
      .option("region", kinesisRegion)
      .load()
      .withColumn('value',col("data").cast(StringType()))                
      .withColumn('key',col('partitionKey').cast(DoubleType()))
      .select('key','value')
    )

# COMMAND ----------

# DBTITLE 1,Add View for historical data
@dlt.create_view(
   name = 'turbine_historical_data',
)
def historical_data():
  return (
    spark.readStream.table('volker_windturbine.turbine_bronze_historical')
  )

# COMMAND ----------

# DBTITLE 1,Stream data from bronze layer into the silver layer: Add data transformation and expectations
jsonSchema = StructType([StructField(col, DoubleType(), False) for col in ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10", "SPEED", "TORQUE", "ID"]] + [StructField("TIMESTAMP", TimestampType())])
@dlt.create_table(
  comment="Turbine dataset with cleaned-up datatypes / column names and quality expectations.",
  table_properties={
    "quality": "silver"
  }
)
@dlt.expect_or_fail("TIMESTAMP Constraint", "TIMESTAMP IS NOT NULL")
@dlt.expect("SPEED constraint", "SPEED >= 0")
@dlt.expect_or_drop("ID", "ID > 0")
def turbine_silver():
  return (
    dlt.read_stream("turbine_bronze_kinesis")
    .union(dlt.read_stream("turbine_historical_data"))
    .withColumn("jsonData", from_json(col("value"), jsonSchema)).select("jsonData.*") 
  )

# COMMAND ----------

# DBTITLE 1,Read external data source from Redshift
hostname = dbutils.secrets.get( "oetrta", "redshift-hostname" )
port     = dbutils.secrets.get( "oetrta", "redshift-port"     )
database = dbutils.secrets.get( "oetrta", "redshift-database" )
username = dbutils.secrets.get( "oetrta", "redshift-username" )
password = dbutils.secrets.get( "oetrta", "redshift-password" )
tempdir  = dbutils.secrets.get( "oetrta", "redshift-temp-dir" )
iam_role = dbutils.secrets.get( "oetrta", "redshift-iam-role" )
redshift_url = f"jdbc:postgresql://{hostname}:{port}/{database}?user={username}&password={password}"
  
@dlt.create_view(
  comment="A table of the device status"
)
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

# DBTITLE 1,Build Gold data by joining the device status with the ingested IoT data
@dlt.create_table(
  comment="The list of devices with their status",
  table_properties={
    "quality": "gold"
  }
 )
def turbine_gold():
  return  dlt.read_stream("turbine_silver").join(dlt.read("device_status"), ['id'], 'left')
