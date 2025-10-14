# Databricks notebook source
from pyspark.sql.functions import col, when, timestamp_diff, lit
from pyspark.sql.types import DoubleType


# COMMAND ----------

hv_fhv_trips_df = spark.read.table("nyctaxi.01_bronze.hv_fhv_trips_raw")

# COMMAND ----------

hv_fhv_trips_df=hv_fhv_trips_df.filter("pickup_datetime >= '2024-12-01' and pickup_datetime <= '2025-07-31'")


# COMMAND ----------

hv_fhv_trips_df =hv_fhv_trips_df.select (
    'hvfhs_license_num',
    'dispatching_base_num',
    'originating_base_num',
    'request_datetime',
    'on_scene_datetime',
    'pickup_datetime',
    'dropoff_datetime',
    timestamp_diff('MINUTE', col("pickup_datetime"), col("dropOff_datetime")).alias("trip_duration"),
    
    col('PULocationID').alias('pu_location_id'),
    col('DOLocationID').alias('do_location_id'),
    'trip_miles',
    'trip_time',
    'base_passenger_fare',
    'tolls',
    'bcf',
    'sales_tax',
    'congestion_surcharge',
    'airport_fee',
    'tips',
    'driver_pay',
    'shared_request_flag',
    'shared_match_flag',
    'access_a_ride_flag',
    'wav_request_flag',
    'wav_match_flag',
    'cbd_congestion_fee',
    'taxi_type',
    'processed_timestamp',
    'source_file_name'
)

# COMMAND ----------

hv_fhv_trips_df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.hv_fhv_trips_cleansed")
