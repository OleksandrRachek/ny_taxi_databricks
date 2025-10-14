# Databricks notebook source
#import functions and relativedelta for date calculations

import sys
import os

# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)



from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import current_timestamp, col,right
from datetime import date

from modules.transformations.metadata import add_file_name, add_processed_timestamp
from modules.utils.date_utils import get_target_yyyymm

# COMMAND ----------

#obtain the YYYY-MM for 2 months prior to the current month in YYYY-MM format

formatted_date = get_target_yyyymm(2)

# COMMAND ----------

#read parquet file for the particular month from the landing directory to DF
taxi_df = spark.read.format("parquet").load(f"/Volumes/nyctaxi/00_landing/data_sources/nytaxi_hvfhv/{formatted_date}")

# COMMAND ----------

#add extra columns - processed_timestamp and file_name

taxi_df = taxi_df.withColumn("taxi_type", lit("high volume for-hire vehicle"))
taxi_df = add_processed_timestamp(taxi_df)
taxi_df = add_file_name(taxi_df)

# COMMAND ----------

#write to the table yellow_trips_raw -- APPEND MODE
taxi_df.write.mode("append").saveAsTable("nyctaxi.01_bronze.hv_fhv_trips_raw")
