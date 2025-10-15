# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import count, max, min, avg, sum, round, col
from dateutil.relativedelta import relativedelta
from datetime import date
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = get_month_start_n_months_ago(2)

# COMMAND ----------

# Load the enriched trip dataset 
# and filter to only include trips with a pickup datetime later than the start date from two months ago
df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime > '{two_months_ago_start}'")

# COMMAND ----------

#aggregations
df =df.filter ((col("trip_distance")<1) & (col("total_amount")>50))

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi.03_gold.daily_fraud_detection")
