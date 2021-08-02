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

dbutils.widgets.text('reset_all_data',"true")
dbutils.widgets.text('path',"/home/volker.tjaden@databricks.com/turbine/")
dbutils.widgets.text("dbName", "volker_windturbine")

# COMMAND ----------

# DBTITLE 1,Let's prepare our data first
# MAGIC %run ./00-setup_power $reset_all=$reset_all_data $dbName=dbName $path=$path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Bronze layer: Ingest historical data

# COMMAND ----------

# MAGIC %fs ls /mnt/oetrta/volker/turbine/incoming-data/

# COMMAND ----------

# DBTITLE 1,Let us ingest some historical raw data first: (key, json)
# MAGIC %sql 
# MAGIC select * from parquet.`/mnt/oetrta/volker/turbine/incoming-data/`;

# COMMAND ----------

# DBTITLE 1,Now, let us switch to our ingest pipeline for historical data
# MAGIC %md
# MAGIC ### [Delta Pipeline for batch ingest](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/4bb477c8-b16a-4c22-a571-9baebb75e8bc)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists turbine_bronze (key double not null, value string) using delta ;

# COMMAND ----------

#Option 1, read from files instead
bronzeDF = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.maxFilesPerTrigger", 1) \
                .schema("value string, key double") \
                .load("/mnt/oetrta/volker/turbine/incoming-data/") 
                  
bronzeDF.writeStream \
        .option("ignoreChanges", "true") \
        .trigger(once=True) \
        .table("turbine_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Now setup: Pipeline for live data

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Notebook with definitions](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/2086664449477884/command/2086664449477887)
# MAGIC ### [Link to Pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/ced62127-a0ad-47b4-8965-31ca3d25ca47)

# COMMAND ----------

# MAGIC %md
# MAGIC Some code to inspect the different parts

# COMMAND ----------

# DBTITLE 1,In addition, we will be pulling Data from Kinesis stream
kinesis_readings = (spark
  .readStream
  .format("kinesis")
  .option("streamName", my_stream_name)
  .option("initialPosition", "latest")
  .option("region", kinesisRegion)
  .load()
  .withColumn('value',col("data").cast(StringType()))                
  .withColumn('key',col('partitionKey').cast(DoubleType()))         
  #.withColumn('json',from_json(col('payload'),jsonSchema))
                   )

# COMMAND ----------

display(kinesis_readings)

# COMMAND ----------

(kinesis_readings
 .select('key','value')
 .writeStream
 .table("turbine_bronze")
)

# COMMAND ----------

# DBTITLE 1,Our raw data is now available in a Delta table, without having small files issues & with great performances
# MAGIC %sql
# MAGIC select * from turbine_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Silver layer: transform JSON data into tabular table

# COMMAND ----------

jsonSchema = StructType([StructField(col, DoubleType(), False) for col in ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10", "SPEED", "TORQUE", "ID"]] + [StructField("TIMESTAMP", TimestampType())])

spark.readStream.table('turbine_bronze') \
     .withColumn("jsonData", from_json(col("value"), jsonSchema)) \
     .select("jsonData.*") \
     .writeStream \
     .option("ignoreChanges", "true") \
     .format("delta") \
     .trigger(processingTime='10 seconds') \
     .table("turbine_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- let's add some constraints in our table, to ensure or ID can't be negative (need DBR 7.5)
# MAGIC ALTER TABLE turbine_silver ADD CONSTRAINT idGreaterThanZero CHECK (id >= 0);
# MAGIC -- let's enable the auto-compaction
# MAGIC alter table turbine_silver set tblproperties ('delta.autoOptimize.autoCompact' = true, 'delta.autoOptimize.optimizeWrite' = true);
# MAGIC 
# MAGIC -- Select data
# MAGIC select * from turbine_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Gold layer: join information on Turbine status to add a label to our dataset (load data from Redshift)

# COMMAND ----------

status_df =( spark.read 
            .format( "com.databricks.spark.redshift") 
            .option( "url",          redshift_url   ) 
            .option( "dbtable",      "turbine_status"         ) 
            .option( "tempdir",      tempdir        ) 
            .option( "aws_iam_role", iam_role       )
            .load()
          )

# COMMAND ----------

display(status_df)

# COMMAND ----------

turbine_stream = spark.readStream.table('turbine_silver')

turbine_stream.join(status_df, ['id'], 'left') \
              .writeStream \
              .option("ignoreChanges", "true") \
              .format("delta") \
              .trigger(processingTime='10 seconds') \
              .table("turbine_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Select data
# MAGIC select * from turbine_gold;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Run DELETE/UPDATE/MERGE with DELTA ! 
# MAGIC We just realized that something is wrong in the data before 2020! Let's DELETE all this data from our gold table as we don't want to have wrong value in our dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM turbine_gold where timestamp < '2020-00-01';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY turbine_gold;
# MAGIC -- If needed, we can go back in time to select a specific version or timestamp
# MAGIC -- SELECT * FROM turbine_gold TIMESTAMP AS OF '2020-12-01'
# MAGIC 
# MAGIC -- And restore a given version
# MAGIC -- RESTORE turbine_gold TO TIMESTAMP AS OF '2020-12-01'
# MAGIC 
# MAGIC -- Or clone the table (zero copy)
# MAGIC -- CREATE TABLE turbine_gold_clone [SHALLOW | DEEP] CLONE turbine_gold VERSION AS OF 32

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Our data is ready! Let's create a dashboard to monitor our Turbine plant
# MAGIC 
# MAGIC https://e2-demo-west.cloud.databricks.com/sql/dashboards/a81f8008-17bf-4d68-8c79-172b71d80bf0-turbine-demo?o=2556758628403379
