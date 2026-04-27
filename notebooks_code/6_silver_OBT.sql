-- Databricks notebook source
USE CATALOG uber_catalog;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.silver_obt
USING DELTA
OPTIONS (path "abfss://silver@uberprojdl.dfs.core.windows.net/silver_obt")
AS 

SELECT 
  rides.*,
  mvm.vehicle_make ,      
  mvt.vehicle_type,mvt.description,mvt.base_rate,mvt.per_mile,mvt.per_minute ,        
  mrs.ride_status  ,       
  mpm.payment_method, mpm.is_card, mpm.requires_auth,       
  mc.city as pickup_city, mc.state, mc.region ,
  mcr.cancellation_reason 
   
FROM 
         
  silver.uber_rides AS rides      
  LEFT JOIN bronze.map_vehicle_makes mvm
  ON rides.vehicle_make_id = mvm.vehicle_make_id           
      
  LEFT JOIN bronze.map_vehicle_types mvt
  ON rides.vehicle_type_id = mvt.vehicle_type_id       

  LEFT JOIN bronze.map_ride_statuses mrs 
  ON rides.ride_status_id = mrs.ride_status_id
      

  LEFT JOIN bronze.map_payment_methods mpm 
  ON rides.payment_method_id = mpm.payment_method_id            


  LEFT JOIN bronze.map_cities mc 
  ON rides.pickup_city_id = mc.city_id

  LEFT JOIN bronze.map_cancellation_reasons mcr 
  ON rides.cancellation_reason_id = mcr.cancellation_reason_id;

    
