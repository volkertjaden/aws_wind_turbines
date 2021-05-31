# Databricks notebook source
# MAGIC %md # Delta Generated Columns & buckets
# MAGIC Delta now supports Generated Columns syntax to specify how a column is computed from other columns.
# MAGIC A generated column is a special column that’s defined with a SQL expression when creating a table.
# MAGIC 
# MAGIC This is useful to leverage partitions by applying filter on a derivated column. It's also being used to generate buckets in your tables to speedup your joins

# COMMAND ----------

# DBTITLE 1,Let's prepare our data first
# MAGIC %run ./resources/00.0-setup-bucketing $reset_all_data=$reset_all_data

# COMMAND ----------

# DBTITLE 1,Let's create a table partitioned by a GENERATED column, using a standard spark expression
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS turbine_silver_partitioned 
# MAGIC   (AN3 DOUBLE, AN4 DOUBLE, AN5 DOUBLE, AN6 DOUBLE, AN7 DOUBLE, AN8 DOUBLE, AN9 DOUBLE, AN10 DOUBLE, SPEED DOUBLE, TORQUE DOUBLE, ID DOUBLE, TIMESTAMP TIMESTAMP,
# MAGIC   yymmdd date GENERATED ALWAYS AS ( CAST(TIMESTAMP AS DATE) )) 
# MAGIC   USING delta;
# MAGIC   
# MAGIC insert into turbine_silver_partitioned (AN3, AN4, AN5, AN6, AN7, AN8, AN9, AN10, SPEED, TORQUE, ID, TIMESTAMP) SELECT * FROM turbine_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC We can now add filter on the TIMESTAMP column (typically what you'd use to filter your data from a BI tool), and the filter will be pushed down at the partition level:

# COMMAND ----------

# MAGIC %python 
# MAGIC # filtering on the TIMESTAMP now push down the filter to the yymmdd partition: PartitionFilters: [(yymmdd#4702 >= cast(2020-05-09 15:17:05 as date))]
# MAGIC print(spark.sql("""explain select * from turbine_silver_partitioned where TIMESTAMP > '2020-05-09 15:17:05'""").first()[0])

# COMMAND ----------

# MAGIC %md ## Improving join performance with table bucketing
# MAGIC Table bucketing leverage generated column to add an extra layer on your table layout (folders like partition).
# MAGIC 
# MAGIC If both of your tables are bucketed by the field you're performing the join, the shuffle will disappear in the join as the data is already bucketed

# COMMAND ----------

# DBTITLE 1,Let's explore what is being delivered by our wind turbines stream: (json)
# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS turbine_silver_bucket
# MAGIC   (AN3 DOUBLE, AN4 DOUBLE, AN5 DOUBLE, AN6 DOUBLE, AN7 DOUBLE, AN8 DOUBLE, AN9 DOUBLE, AN10 DOUBLE, SPEED DOUBLE, TORQUE DOUBLE, ID DOUBLE, TIMESTAMP TIMESTAMP,
# MAGIC   yymmdd date GENERATED ALWAYS AS ( CAST(TIMESTAMP AS DATE) )) 
# MAGIC   USING delta 
# MAGIC   CLUSTERED by (id) into 16 buckets ;
# MAGIC   
# MAGIC insert into turbine_silver_bucket (AN3, AN4, AN5, AN6, AN7, AN8, AN9, AN10, SPEED, TORQUE, ID, TIMESTAMP) SELECT * FROM turbine_silver;

# COMMAND ----------

# MAGIC %md #### Group By operations

# COMMAND ----------

# DBTITLE 1,Non bucketed plan. Note the "Exchange hashpartitioning" stage
# MAGIC %python display_plan("select id, count(*) from turbine_silver group by id")

# COMMAND ----------

# DBTITLE 1,Bucketed, no "Exchange" stage
# MAGIC %python display_plan("select id, count(*) from turbine_silver_bucket group by id")

# COMMAND ----------

# MAGIC %md #### Bucketing also works for joins, window operation etc
# MAGIC 
# MAGIC **Result at scale: up to 5x JOIN speedup with DBR8; up to 50%+ with Photon**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists turbine_status_gold_bucket 
# MAGIC     (id int, status string) 
# MAGIC   using delta 
# MAGIC   CLUSTERED by (id) into 16 buckets ;
# MAGIC 
# MAGIC insert into turbine_status_gold_bucket (id, status)  SELECT * FROM turbine_status_gold;

# COMMAND ----------

# DBTITLE 1,Non bucketed plan. Note the "Exchange hashpartitioning" stage
# MAGIC %python 
# MAGIC display_plan("select /*+ SHUFFLE_MERGE(status) */ * from turbine_status_gold status inner join turbine_silver data on status.id = data.id")
# MAGIC #Note: we're adding SHUFFLE_MERGE(status) hint to disable broadcast hashjoin as it's a small table

# COMMAND ----------

# DBTITLE 1,Bucketed, no "Exchange" stage
# MAGIC %python display_plan("select /*+ SHUFFLE_MERGE(status) */ * from turbine_status_gold_bucket status inner join turbine_silver_bucket data on status.id = data.id")
# MAGIC #Note: we're adding SHUFFLE_MERGE(status) hint to disable broadcast hashjoin as it's a small table
