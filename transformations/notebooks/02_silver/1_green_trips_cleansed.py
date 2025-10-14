# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)


from pyspark.sql.functions import col, when, timestamp_diff, lit
from pyspark.sql.types import DoubleType
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

green_trips_df = spark.read.table("nyctaxi.01_bronze.green_trips_raw").filter(f"lpep_pickup_datetime >= '{two_months_ago_start}' AND lpep_pickup_datetime < '{one_month_ago_start}'")

# COMMAND ----------

green_trips_df = green_trips_df.select(
    #Map VendorID into Vendor names
    when (col("VendorID") == 1, 'Creative Mobile Technologies, LLC')
      .when (col("VendorID") == 2, 'Curb Mobility, LLC')
      .when(col("VendorID") == 6, 'Myle Technologies Inc')
      .otherwise('Unknown').alias("vendor"),
    
    col("lpep_pickup_datetime").alias("pickup_datetime"),
    col("lpep_dropoff_datetime").alias("dropoff_datetime"),
    #calculate trip duration in minutes
    timestamp_diff('MINUTE', col("lpep_pickup_datetime"), col("lpep_dropoff_datetime")).alias("trip_duration"),
    "passenger_count",
    "trip_distance",
    #decode rate codes into readable rate types
    when(col("RatecodeID") == 1, 'Standard Rate')
      .when(col("RatecodeID") == 2, "JFK")
      .when(col("RatecodeID") == 3, 'Newark')
      .when(col("RatecodeID") == 4, 'Nassau or Westchester')
      .when(col("RatecodeID") == 5, 'Negotiated Fare')
      .when(col("RatecodeID") == 6, 'Group Ride')
      .otherwise('Unknown').alias("rate_type"),
      "store_and_fwd_flag",
      #alias columns for consistent naming convention
      col("PULocationID").alias("pu_location_id"),
      col("DOLocationID").alias("do_location_id"),
      #decode payment types
      when(col("payment_type") == 0, 'Flex Fare trip')
        .when(col("payment_type") == 1, 'Credit card')
        .when(col("payment_type") == 2, 'Cash')
        .when(col("payment_type") == 3, 'No charge')
        .when(col("payment_type") == 4, 'Dispute')
        .when(col("payment_type") == 6, 'Voided trip')
        .otherwise('Unknown').alias("payment_type"),
      "fare_amount",
      "extra",
      "mta_tax",
      "tip_amount",
      "tolls_amount",
      "improvement_surcharge",
      "total_amount",
      "congestion_surcharge",
      #alias for columns
      lit(None).cast(DoubleType()).alias("airport_fee"),
      "cbd_congestion_fee",
      when(col("trip_type") == 1, 'Street-hall')
        .when(col("trip_type") == 2, 'Dispatch')
        .otherwise('Unknown').alias("trip_type"),
      "taxi_type",
      "processed_timestamp",
      "source_file_name")

# COMMAND ----------

green_trips_df.write.mode("append").saveAsTable("nyctaxi.02_silver.green_trips_cleansed")

# COMMAND ----------


