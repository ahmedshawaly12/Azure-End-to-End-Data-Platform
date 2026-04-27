-- Databricks notebook source
USE CATALOG uber_catalog;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver.uber_rides
USING DELTA
LOCATION 'abfss://silver@uberprojdl.dfs.core.windows.net/ride_events'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true'
)
AS
SELECT *, 
current_timestamp() as ingested_at FROM bronze.bulk_rides

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS metadata;

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS metadata.pipeline_watermark (
    table_name      STRING    NOT NULL,
    last_version    BIGINT,
    last_updated    TIMESTAMP
)
USING DELTA
LOCATION 'abfss://raw@uberprojdl.dfs.core.windows.net/metadata/pipeline_watermark';

-- COMMAND ----------

INSERT INTO metadata.pipeline_watermark
SELECT
    'bronze.ride_events'        AS table_name,
    MAX(version)                AS last_version,
    current_timestamp()         AS last_updated
FROM (DESCRIBE HISTORY bronze.ride_events);