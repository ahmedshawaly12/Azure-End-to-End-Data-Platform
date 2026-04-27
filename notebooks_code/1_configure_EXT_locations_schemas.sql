-- Databricks notebook source
SELECT current_metastore();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create External Locations

-- COMMAND ----------

 -- External Location To bronze contianer

CREATE EXTERNAL LOCATION IF NOT EXISTS uberprojdl_raw
    URL "abfss://raw@uberprojdl.dfs.core.windows.net/"
    WITH (STORAGE CREDENTIAL `uber-proj-sc`)
    COMMENT 'External Location for raw container';

-- COMMAND ----------

-- MAGIC  %fs ls "abfss://raw@uberprojdl.dfs.core.windows.net/"

-- COMMAND ----------

  -- External Location To silver contianer
  
  CREATE EXTERNAL LOCATION IF NOT EXISTS uberprojdl_silver
    URL "abfss://silver@uberprojdl.dfs.core.windows.net/"
    WITH (STORAGE CREDENTIAL `uber-proj-sc`)
    COMMENT 'External Location for silver container';

-- COMMAND ----------

-- MAGIC %fs ls "abfss://silver@uberprojdl.dfs.core.windows.net/"

-- COMMAND ----------

-- External Location to gold container

  CREATE EXTERNAL LOCATION IF NOT EXISTS uberprojdl_gold
    URL "abfss://gold@uberprojdl.dfs.core.windows.net/"
    WITH (STORAGE CREDENTIAL `uber-proj-sc`)
    COMMENT 'External Location for gold container';

-- COMMAND ----------

-- MAGIC %fs ls "abfss://gold@uberprojdl.dfs.core.windows.net/"

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS uber_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Schemas - bronze, silver, gold with managed location

-- COMMAND ----------

USE CATALOG uber_catalog;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION "abfss://raw@uberprojdl.dfs.core.windows.net/";

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION "abfss://silver@uberprojdl.dfs.core.windows.net/";

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION "abfss://gold@uberprojdl.dfs.core.windows.net/";

-- COMMAND ----------

SHOW SCHEMAS;