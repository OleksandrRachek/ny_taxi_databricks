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

fhv_trips_df = spark.read.table("nyctaxi.01_bronze.fhv_trips_raw").filter(f"pickup_datetime >= '{two_months_ago_start}' AND pickup_datetime < '{one_month_ago_start}'")

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

fhv_trips_df.write.mode("append").saveAsTable("nyctaxi.02_silver.fhv_trips_cleansed")

# COMMAND ----------


