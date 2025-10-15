# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import count, max, min, avg, sum, round, lit, col, mean
from pyspark.sql.types import IntegerType
from dateutil.relativedelta import relativedelta
from datetime import date
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = get_month_start_n_months_ago(2)

# COMMAND ----------

# Load the enriched trip dataset 
# and filter to only include trips with a pickup datetime later than the start date from two months ago
df_green_yellow = spark.read.table("nyctaxi.02_silver.green_and_yellow_trips_enriched").filter(f"pickup_datetime > '{two_months_ago_start}'")
df_fhv = spark.read.table("nyctaxi.02_silver.fhv_trips_enriched").filter(f"pickup_datetime > '{two_months_ago_start}'")
df_hv_fhv = spark.read.table("nyctaxi.02_silver.hv_fhv_trips_enriched").filter(f"pickup_datetime > '{two_months_ago_start}'")


# COMMAND ----------

#Select only appropriate columns
df_green_yellow = (
    df_green_yellow.select(
        df_green_yellow.pickup_datetime,
        df_green_yellow.pu_zone,
        df_green_yellow.do_zone,
        df_green_yellow.passenger_count,
        df_green_yellow.trip_distance,
        df_green_yellow.fare_amount,
        df_green_yellow.total_amount,
        df_green_yellow.taxi_type
    )
)
    

# COMMAND ----------

#Select only appropriate columns
df_hv_fhv = (
    df_hv_fhv.select(
        df_hv_fhv.pickup_datetime,
        df_hv_fhv.pu_zone,
        df_hv_fhv.do_zone,
        lit(1).cast(IntegerType()).alias('passenger_count'),
        df_hv_fhv.trip_miles.alias('trip_distance'),
        df_hv_fhv.base_passenger_fare.alias('fare_amount'),
        df_hv_fhv.driver_pay.alias('total_amount'),
        df_hv_fhv.taxi_type

    )
)

# COMMAND ----------

#Select only appropriate columns
df_fhv = df_fhv.select(
    df_fhv.pickup_datetime,
    df_fhv.pu_zone,
    df_fhv.do_zone,
    df_fhv.passenger_count,
    lit(None).cast(IntegerType()).alias('trip_distance'),
    lit(None).cast(IntegerType()).alias('fare_amount'),
    lit(None).cast(IntegerType()).alias('total_amount'),
    df_fhv.taxi_type
)

# COMMAND ----------

#union
df_union = df_green_yellow.union(df_hv_fhv).union(df_fhv)

# COMMAND ----------

#calculate means for trip distance, fare amount and total amount
mean_for_fill =df_union.agg(mean("trip_distance").alias("trip_distance"), mean("fare_amount").alias("fare_amount"), mean("total_amount").alias("total_amount"))
mean_dict = mean_for_fill.first().asDict()

# COMMAND ----------

#fill na 
df_union = df_union.fillna(mean_dict)

# COMMAND ----------

df_union.write.mode("append").saveAsTable("nyctaxi.02_silver._factTrips")
