-- Databricks notebook source
USE CATALOG uber_catalog;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC last_version = spark.sql("""
-- MAGIC     SELECT last_version
-- MAGIC     FROM metadata.pipeline_watermark
-- MAGIC     WHERE table_name = 'bronze.ride_events'
-- MAGIC """).collect()[0][0]
-- MAGIC
-- MAGIC
-- MAGIC latest_bronze_version = spark.sql("""
-- MAGIC     SELECT MAX(version) AS latest_version
-- MAGIC     FROM (DESCRIBE HISTORY bronze.ride_events)
-- MAGIC """).collect()[0][0]
-- MAGIC
-- MAGIC print(f"Watermark version : {last_version}")
-- MAGIC print(f"Latest Bronze version : {latest_bronze_version}")
-- MAGIC
-- MAGIC
-- MAGIC if last_version >= latest_bronze_version:
-- MAGIC     print(" No new Bronze changes since last run — skipping merge.")
-- MAGIC     dbutils.notebook.exit("NO_NEW_DATA")
-- MAGIC else:
-- MAGIC     print(f" Reading Bronze changes from version: {last_version + 1}")
-- MAGIC     (
-- MAGIC         spark.read.format("delta")
-- MAGIC             .option("readChangeFeed", "true")
-- MAGIC             .option("startingVersion", last_version + 1)
-- MAGIC             .table("uber_catalog.bronze.ride_events")
-- MAGIC             .createOrReplaceTempView("ride_cdf_raw")
-- MAGIC     )
-- MAGIC     print(" bronze_cdf_raw temp view created successfully.")

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW inc_rides AS
SELECT *
FROM (
    SELECT
        ride_id,
        confirmation_number,
        passenger_id,
        driver_id,
        vehicle_id,
        pickup_location_id,
        dropoff_location_id,
        vehicle_type_id,
        vehicle_make_id,
        payment_method_id,
        ride_status_id,
        pickup_city_id,
        dropoff_city_id,
        cancellation_reason_id,
        passenger_name,
        passenger_email,
        passenger_phone,
        driver_name,
        driver_rating,
        driver_phone,
        driver_license,
        vehicle_model,
        vehicle_color,
        license_plate,
        pickup_address,
        pickup_latitude,
        pickup_longitude,
        dropoff_address,
        dropoff_latitude,
        dropoff_longitude,
        distance_miles,
        duration_minutes,
        booking_timestamp,
        pickup_timestamp,
        dropoff_timestamp,
        base_fare,
        distance_fare,
        time_fare,
        surge_multiplier,
        subtotal,
        tip_amount,
        total_fare,
        rating,
        ingested_at ,            


        ROW_NUMBER() OVER (
            PARTITION BY ride_id
            ORDER BY ingested_at DESC
        ) AS row_num

    FROM ride_cdf_raw
    WHERE _change_type IN ('insert', 'update_postimage')
      AND ride_id       IS NOT NULL
)
WHERE row_num = 1;

-- COMMAND ----------

MERGE INTO silver.uber_rides    AS silver
USING inc_rides        AS bronze
ON silver.ride_id = bronze.ride_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *

-- COMMAND ----------

MERGE INTO metadata.pipeline_watermark AS w
USING (
    SELECT
        'bronze.ride_events'    AS table_name,
        MAX(version)            AS last_version,
        current_timestamp()     AS last_updated
    FROM (DESCRIBE HISTORY bronze.ride_events)
    WHERE operation IN ('WRITE', 'STREAMING UPDATE', 'MERGE')
) AS latest
ON w.table_name = latest.table_name
WHEN MATCHED THEN
    UPDATE SET
        w.last_version = latest.last_version,
        w.last_updated = latest.last_updated;