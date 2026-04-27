-- Databricks notebook source
USE CATALOG uber_catalog;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze.bulk_rides
(
    -- Identifiers
    ride_id                 STRING,
    confirmation_number     STRING,
    passenger_id            STRING,
    driver_id               STRING,
    vehicle_id              STRING,
    pickup_location_id      STRING,
    dropoff_location_id     STRING,

    -- Foreign Key IDs
    vehicle_type_id         INT,
    vehicle_make_id         INT,
    payment_method_id       INT,
    ride_status_id          INT,
    pickup_city_id          INT,
    dropoff_city_id         INT,
    cancellation_reason_id  INT,

    -- Passenger Info
    passenger_name          STRING,
    passenger_email         STRING,
    passenger_phone         STRING,

    -- Driver Info
    driver_name             STRING,
    driver_rating           DOUBLE,
    driver_phone            STRING,
    driver_license          STRING,

    -- Vehicle Info
    vehicle_model           STRING,
    vehicle_color           STRING,
    license_plate           STRING,

    -- Pickup Info
    pickup_address          STRING,
    pickup_latitude         DOUBLE,
    pickup_longitude        DOUBLE,

    -- Dropoff Info
    dropoff_address         STRING,
    dropoff_latitude        DOUBLE,
    dropoff_longitude       DOUBLE,

    -- Trip Metrics
    distance_miles          DOUBLE,
    duration_minutes        INT,

    -- Timestamps
    booking_timestamp       TIMESTAMP,
    pickup_timestamp        TIMESTAMP,
    dropoff_timestamp       TIMESTAMP,

    -- Fare Breakdown
    base_fare               DOUBLE,
    distance_fare           DOUBLE,
    time_fare               DOUBLE,
    surge_multiplier        DOUBLE,
    subtotal                DOUBLE,
    tip_amount              DOUBLE,
    total_fare              DOUBLE,

    -- Rating
    rating                  DOUBLE
)
USING json
OPTIONS (path "abfss://raw@uberprojdl.dfs.core.windows.net/historical_rides/bulk_rides.json",
  multiLine "true" )

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.map_cities
(
  city_id INT,
  city STRING,
  state STRING,
  region STRING
)
USING json
OPTIONS (path "abfss://raw@uberprojdl.dfs.core.windows.net/historical_rides/map_cities.json");

-- COMMAND ----------

SELECT * FROM bronze.map_cities
LIMIT 2;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.map_cancellation_reasons
(
  cancellation_reason_id INT,
  cancellation_reason STRING
)
USING json
OPTIONS (path "abfss://raw@uberprojdl.dfs.core.windows.net/historical_rides/map_cancellation_reasons.json" );

-- COMMAND ----------

SELECT * FROM bronze.map_cancellation_reasons
LIMIT 2;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.map_payment_methods
(
  payment_method_id INT,
  payment_method STRING,
  is_card BOOLEAN,
  requires_auth BOOLEAN
)
USING json
OPTIONS (path "abfss://raw@uberprojdl.dfs.core.windows.net/historical_rides/map_payment_methods.json")

-- COMMAND ----------

SELECT * FROM bronze.map_payment_methods
LIMIT 2;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.map_ride_statuses
(
  ride_status_id INT,
  ride_status STRING,
  is_completed BOOLEAN
)
USING json
OPTIONS (path "abfss://raw@uberprojdl.dfs.core.windows.net/historical_rides/map_ride_statuses.json")

-- COMMAND ----------

SELECT * FROM bronze.map_ride_statuses
LIMIT 2;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.map_vehicle_makes
(
  vehicle_make_id INT,
  vehicle_make STRING
)
USING JSON
OPTIONS (path "abfss://raw@uberprojdl.dfs.core.windows.net/historical_rides/map_vehicle_makes.json");

-- COMMAND ----------

SELECT * FROM bronze.map_vehicle_makes
LIMIT 2;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.map_vehicle_types
(
  vehicle_type_id INT,
  vehicle_type STRING,
  description STRING,
  base_rate FLOAT,
  per_mile FLOAT,
  per_minute FLOAT
)
USING json
OPTIONS ( path "abfss://raw@uberprojdl.dfs.core.windows.net/historical_rides/map_vehicle_types.json") 

-- COMMAND ----------

SELECT * FROM bronze.map_vehicle_types
LIMIT 2;

-- COMMAND ----------

CREATE  OR REPLACE TABLE uber_catalog.bronze.ride_events
(
    -- Identifiers
    ride_id                 STRING,
    confirmation_number     STRING,
    passenger_id            STRING,
    driver_id               STRING,
    vehicle_id              STRING,
    pickup_location_id      STRING,
    dropoff_location_id     STRING,

    -- Foreign Key IDs
    vehicle_type_id         INT,
    vehicle_make_id         INT,
    payment_method_id       INT,
    ride_status_id          INT,
    pickup_city_id          INT,
    dropoff_city_id         INT,
    cancellation_reason_id  INT,

    -- Passenger Info
    passenger_name          STRING,
    passenger_email         STRING,
    passenger_phone         STRING,

    -- Driver Info
    driver_name             STRING,
    driver_rating           DOUBLE,
    driver_phone            STRING,
    driver_license          STRING,

    -- Vehicle Info
    vehicle_model           STRING,
    vehicle_color           STRING,
    license_plate           STRING,

    -- Pickup Info
    pickup_address          STRING,
    pickup_latitude         DOUBLE,
    pickup_longitude        DOUBLE,

    -- Dropoff Info
    dropoff_address         STRING,
    dropoff_latitude        DOUBLE,
    dropoff_longitude       DOUBLE,

    -- Trip Metrics
    distance_miles          DOUBLE,
    duration_minutes        INT,

    -- Timestamps
    booking_timestamp       TIMESTAMP,
    pickup_timestamp        TIMESTAMP,
    dropoff_timestamp       TIMESTAMP,

    -- Fare Breakdown
    base_fare               DOUBLE,
    distance_fare           DOUBLE,
    time_fare               DOUBLE,
    surge_multiplier        DOUBLE,
    subtotal                DOUBLE,
    tip_amount              DOUBLE,
    total_fare              DOUBLE,

    -- Rating
    rating                  DOUBLE,
    ingested_at TIMESTAMP
)
USING delta
OPTIONS (path "abfss://raw@uberprojdl.dfs.core.windows.net/ride_events" )
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- COMMAND ----------

SELECT * FROM uber_catalog.bronze.ride_events;

-- COMMAND ----------

USE SCHEMA bronze;

-- COMMAND ----------

SHOW TABLES;