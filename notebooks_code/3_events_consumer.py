# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

EH_NAMESPACE = "uberprojeventhub"
EH_NAME = "ubertopic"      
EH_CONN_STRING = dbutils.secrets.get("uber-proj-scope", "connectionString")


# COMMAND ----------

JAAS_CONFIG = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="$ConnectionString" '
    f'password="{EH_CONN_STRING}";'
)


BOOTSTRAP_SERVERS = f"{EH_NAMESPACE}.servicebus.windows.net:9093"

kafka_options = {
    "kafka.bootstrap.servers"   : BOOTSTRAP_SERVERS,
    "kafka.security.protocol"   : "SASL_SSL",
    "kafka.sasl.mechanism"      : "PLAIN",
    "kafka.sasl.jaas.config"    : JAAS_CONFIG,
    "subscribe"                 : EH_NAME,
    "startingOffsets"           : "earliest",  
    "kafka.request.timeout.ms"  : "60000",
    "kafka.session.timeout.ms"  : "30000",
    "kafka.max.poll.records"    : "500",
    "failOnDataLoss"            : "false",
}

# COMMAND ----------

raw_stream = spark.readStream.format("kafka").options(**kafka_options).load()


# COMMAND ----------

parsed_stream = raw_stream.select(F.col('value').cast('string'))

# COMMAND ----------

stream_schema = StructType(
    [
     StructField('ride_id', StringType(), True),
      StructField('confirmation_number', StringType(), True),
       StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', IntegerType(), True), StructField('vehicle_make_id', IntegerType(), True), StructField('payment_method_id', IntegerType(), True), StructField('ride_status_id', IntegerType(), True), StructField('pickup_city_id', IntegerType(), True), StructField('dropoff_city_id', IntegerType(), True), StructField('cancellation_reason_id', IntegerType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', IntegerType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', TimestampType(), True), StructField('dropoff_timestamp', TimestampType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)]
    )

# COMMAND ----------

parsed_stream_df = parsed_stream\
    .select(F.from_json(F.col('value'), stream_schema).alias('data'))\
    .select("data.*", F.current_timestamp().alias("ingested_at"))

# COMMAND ----------

checkpointLocation = "abfss://raw@uberprojdl.dfs.core.windows.net/checkpoints/eventhub_events2"

# COMMAND ----------

def write_to_bronze(batch_df, batch_id):
    batch_df.createOrReplaceTempView("streaming_batch")
    spark.sql("""
        INSERT INTO uber_catalog.bronze.ride_events
        SELECT * FROM streaming_batch
        WHERE ride_id IS NOT NULL
    """)

query = parsed_stream_df.writeStream\
        .outputMode("append")\
        .foreachBatch(write_to_bronze)\
        .option("checkpointLocation", checkpointLocation)\
        .trigger(processingTime="5 seconds")\
        .start()


# COMMAND ----------

query.stop()