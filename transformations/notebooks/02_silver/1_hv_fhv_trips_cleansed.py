# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)


from pyspark.sql.functions import col, when, timestamp_diff, lit
from pyspark.sql.types import IntegerType, DoubleType
from datetime import date
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = get_month_start_n_months_ago(2)

# Get the first day of the month one month ago
one_month_ago_start = get_month_start_n_months_ago(1)

# COMMAND ----------

# Read the 'yellow_trips_raw' table from the 'nyctaxi.01_bronze' schema
# Then filter rows where 'tpep_pickup_datetime' is >= two months ago start
# and < one month ago start (i.e., only the month that is two months before today)

hv_fhv_trips_df = spark.read.table("nyctaxi.01_bronze.hv_fhv_trips_raw").filter(f"pickup_datetime >= '{two_months_ago_start}' AND pickup_datetime < '{one_month_ago_start}'")

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

hv_fhv_trips_df.write.mode("append").saveAsTable("nyctaxi.02_silver.hv_fhv_trips_cleansed")

# COMMAND ----------


