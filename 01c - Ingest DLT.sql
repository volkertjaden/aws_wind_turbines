-- Databricks notebook source
CREATE LIVE TABLE turbine_bronze_historical 
COMMENT 'BRONZE TABLE FOR IOT HISTORICAL WIND TURBINE DATA' 
LOCATION '/home/volker.tjaden@databricks.com/turbine/iot_bronze' AS
SELECT
  *
from
  parquet.`/mnt/oetrta/volker/turbine/incoming-data/`
