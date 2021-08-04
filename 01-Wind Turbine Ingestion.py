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
dbutils.widgets.text('path',"s3://oetrta/volker/demos")
dbutils.widgets.text('dbName', 'volker_windturbine')

# COMMAND ----------

# DBTITLE 1,Let's prepare our data first
# MAGIC %run ./00-setup_power $reset_all=$reset_all_data $dbName=$dbName $path=$path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Bronze layer: Ingest historical data

# COMMAND ----------

# MAGIC %fs ls s3://oetrta/volker/datasets/turbine/incoming-data/

# COMMAND ----------

# DBTITLE 1,Let us ingest some historical raw data first: (key, json)
# MAGIC %sql 
# MAGIC select * from parquet.`s3://oetrta/volker/datasets/turbine/incoming-data/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists turbine_bronze (key double not null, value string) using delta ;

# COMMAND ----------

#Option 1, read from files instead
bronzeDF = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .schema("value string, key double") \
                .load("s3://oetrta/volker/datasets/turbine/incoming-data/") 
                  
bronzeDF.writeStream \
        .option("ignoreChanges", "true") \
        .trigger(once=True) \
        .queryName("batch_write") \
        .table("turbine_bronze")

# COMMAND ----------

# DBTITLE 1,Our raw data is now available in a Delta table, without having small files issues & with great performances
# MAGIC %sql
# MAGIC select * from turbine_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Now setup: Pipeline for live data

# COMMAND ----------

# DBTITLE 1,In addition, we will be pulling live data from Kinesis stream
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
 .queryName("kinesis_bronze_write") 
 .table("turbine_bronze")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Silver layer: transform JSON data into tabular table

# COMMAND ----------

jsonSchema = StructType([StructField(col, DoubleType(), False) for col in ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10", "SPEED", "TORQUE", "ID"]] + [StructField("TIMESTAMP", TimestampType())])

silver_stream = (spark.readStream.table('turbine_bronze') \
     .withColumn("jsonData", from_json(col("value"), jsonSchema)) \
     .select("jsonData.*") \
     .writeStream \
     .option("ignoreChanges", "true") \
     .format("delta") \
     .queryName("silver_write") \
     .trigger(processingTime='2 seconds') \
     .table("turbine_silver")
                )

# COMMAND ----------

# DBTITLE 1,First, let us run a static SQL query on Delta Table
# MAGIC %sql
# MAGIC select count(*)
# MAGIC from turbine_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from turbine_silver
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,Now let us run some queries on the consolidated Live data set
live_df = (spark
           .readStream
           .table('turbine_silver')
          )

live_df.createOrReplaceTempView('live_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select ID, count(*)
# MAGIC from live_view
# MAGIC group by ID
# MAGIC order by ID

# COMMAND ----------

from pyspark.sql.functions import current_date, window

(live_df
 .withWatermark("TIMESTAMP", '1 hours')
 .where(col('TIMESTAMP').cast("DATE")==current_date())
 .groupby('ID',window('TIMESTAMP',"10 minutes"))
 .avg('SPEED').alias('mean_velocity')
 .writeStream
 .format('memory')
 .queryName("avg_speed")
 .outputMode("complete")
 .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select ID, window.start, `avg(SPEED)`
# MAGIC from avg_speed
# MAGIC where ID in (506, 33)

# COMMAND ----------

# DBTITLE 1,Add data quality constraints
# MAGIC %sql
# MAGIC ALTER TABLE turbine_silver ADD CONSTRAINT idGreaterThanZero CHECK (id >= 0);
# MAGIC ALTER TABLE turbine_silver ADD CONSTRAINT speedGreaterThanZero CHECK (speed >= 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL turbine_silver

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
# MAGIC # Let us do some live-scoring on a pre-trained model

# COMMAND ----------

# DBTITLE 1,Stop active streams
for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Our data is ready! Let's create a dashboard to monitor our Turbine plant
# MAGIC 
# MAGIC https://e2-demo-west.cloud.databricks.com/sql/dashboards/a81f8008-17bf-4d68-8c79-172b71d80bf0-turbine-demo?o=2556758628403379
