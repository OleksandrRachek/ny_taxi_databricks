# Databricks notebook source
from pyspark.sql.functions import col, when, timestamp_diff, lit
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType


# COMMAND ----------

fhv_trips_df = spark.read.table("nyctaxi.01_bronze.fhv_trips_raw")

# COMMAND ----------

fhv_trips_df=fhv_trips_df.filter("pickup_datetime >= '2024-12-01' and pickup_datetime <= '2025-07-31'")


# COMMAND ----------

fhv_trips_df = fhv_trips_df.select (
    'dispatching_base_num',
    'pickup_datetime',
    'dropOff_datetime',
    timestamp_diff('MINUTE', col("pickup_datetime"), col("dropOff_datetime")).alias("trip_duration"),
    col('PUlocationID').alias('pu_location_id'),
    col('DOlocationID').alias('do_location_id'),
    col('SR_Flag').alias('sr_flag'),
    col('Affiliated_base_number').alias('affiliated_base_number'),
    when(col("sr_flag").isNull(), 1).otherwise(lit(None).cast(IntegerType())).alias("passenger_count"),
    'taxi_type',
    'processed_timestamp',
    'source_file_name')

# COMMAND ----------

fhv_trips_df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.fhv_trips_cleansed")
