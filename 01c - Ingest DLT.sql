-- Databricks notebook source
CREATE LIVE TABLE iot_bronze 
COMMENT 'BRONZE TABLE FOR IOT WIND TURBINE DATA' 
LOCATION '/Users/volker.tjaden@databricks.com/dev/iot_bronze' AS
SELECT
  *
from
  parquet.`/Users/volker.tjaden@databricks.com/dev/wind_sample`

-- COMMAND ----------

CREATE LIVE TABLE iot_silver (
  CONSTRAINT idGreaterThanZero EXPECT (id >= 0) ON VIOLATION DROP ROW
)
COMMENT 'SILVER TABLE FOR IOT WIND TURBINE DATA' 
LOCATION '/Users/volker.tjaden@databricks.com/dev/iot_silver' AS

SELECT
  json.*
FROM
  (
    SELECT
      from_json(
        value,
        'AN3 double, AN4 double, AN5 double, AN6 double, AN7 double, AN8 double, AN9  double, AN10 double, SPEED double, TORQUE double, ID double, TIMESTAMP timestamp'
      ) AS json
    FROM
      (
        (SELECT * FROM live.iot_bronze)
        UNION
        (SELECT * FROM volker_dev.iot_bronze_kinesis)
      )
      
  )
