# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import count, max, min, avg, sum, round, date_format, hour
from dateutil.relativedelta import relativedelta
from datetime import date
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = get_month_start_n_months_ago(2)

# COMMAND ----------

# Load the enriched trip dataset 
# and filter to only include trips with a pickup datetime later than the start date from two months ago
df_trips = spark.read.table("nyctaxi.02_silver._factTrips").filter(f"pickup_datetime > '{two_months_ago_start}'")


# COMMAND ----------

#aggregations
df_trips = (
    df_trips.
        # group records by calendar date
        groupBy(date_format("pickup_datetime", "yyyy-MM").alias("year_month"), hour(df_union.pickup_datetime).alias("pickup_hour"), df_union.pu_zone ). 
        agg(
            count("*").alias("total_trips"),                             # total number of trips per day
            round(avg("passenger_count"), 1).alias("average_passengers"), # average passengers per trip
            round(avg("trip_distance"), 1).alias("average_distance"),     # average trip distance (miles)
            round(avg("fare_amount"), 2).alias("average_fare_per_trip"),   # average fare per trip ($)
            max("fare_amount").alias("max_fare"),                         # highest single-trip fare
            min("fare_amount").alias("min_fare"),                         # lowest single-trip fare
            round(sum("total_amount"), 2).alias("total_revenue")          # total revenue for the day ($)
        )
)

# COMMAND ----------


df_trips.write.mode("append").saveAsTable("nyctaxi.03_gold.hourly_trip_summary_per_pu_zone")
