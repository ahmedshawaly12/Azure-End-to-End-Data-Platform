-- Databricks notebook source
USE CATALOG uber_catalog;
USE SCHEMA gold;

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_passenger 
USING DELTA
OPTIONS (path "abfss://gold@uberprojdl.dfs.core.windows.net/dim_passenger")
AS
SELECT DISTINCT
  passenger_id,
  passenger_name,
  passenger_email,
  passenger_phone
FROM silver.silver_obt;

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_driver
USING DELTA
OPTIONS (path "abfss://gold@uberprojdl.dfs.core.windows.net/dim_driver")
AS
SELECT DISTINCT
  driver_id,
  driver_name,
  driver_rating,
  driver_phone,
  driver_license
FROM silver.silver_obt;

-- COMMAND ----------


CREATE OR REPLACE TABLE dim_vehicle 
USING DELTA
OPTIONS (path "abfss://gold@uberprojdl.dfs.core.windows.net/dim_vehicle")
AS
SELECT DISTINCT
  vehicle_id,
  vehicle_make_id,
  vehicle_type_id,
  vehicle_model,
  vehicle_color,
  license_plate,
  vehicle_make,
  vehicle_type
FROM silver.silver_obt;



-- COMMAND ----------


CREATE OR REPLACE TABLE dim_payment 
USING DELTA
OPTIONS (path "abfss://gold@uberprojdl.dfs.core.windows.net/dim_payment")
AS
SELECT DISTINCT
  payment_method_id,
  payment_method,
  is_card,
  requires_auth
FROM silver.silver_obt;



-- COMMAND ----------


CREATE OR REPLACE TABLE dim_booking 
USING DELTA
OPTIONS (path "abfss://gold@uberprojdl.dfs.core.windows.net/dim_booking")
AS
SELECT DISTINCT
  ride_id,
  confirmation_number,
  dropoff_location_id,
  ride_status_id,
  dropoff_city_id,
  cancellation_reason_id,
  dropoff_address,
  dropoff_latitude,
  dropoff_longitude,
  booking_timestamp,
  dropoff_timestamp,
  pickup_address,
  pickup_latitude,
  pickup_longitude,
  pickup_location_id
FROM silver.silver_obt;



-- COMMAND ----------


CREATE OR REPLACE TABLE dim_location 
USING DELTA
OPTIONS (path "abfss://gold@uberprojdl.dfs.core.windows.net/dim_location")
AS
SELECT DISTINCT
  pickup_city_id,
  pickup_city,
  region,
  state
FROM silver.silver_obt



-- COMMAND ----------


CREATE OR REPLACE TABLE fact_rides 
USING DELTA
OPTIONS (path "abfss://gold@uberprojdl.dfs.core.windows.net/fact_rides")
AS
SELECT
  ride_id,
  pickup_city_id,
  payment_method_id,
  driver_id,
  passenger_id,
  vehicle_id,
  distance_miles,
  duration_minutes,
  base_fare,
  distance_fare,
  time_fare,
  surge_multiplier,
  total_fare,
  tip_amount,
  rating,
  base_rate,
  per_mile,
  per_minute
FROM silver.silver_obt;