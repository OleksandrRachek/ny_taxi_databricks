# Databricks notebook source
#import functions and relativedelta for date calculations
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import current_timestamp, col,right
from datetime import date

# COMMAND ----------

#obtain the YYYY-MM for 2 months prior to the current month in YYYY-MM format
two_months_ago = date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime("%Y-%m")

# COMMAND ----------

#read parquet file for the particular month from the landing directory to DF
taxi_df = spark.read.format("parquet").load(f"/Volumes/nyctaxi/00_landing/data_sources/nytaxi_yellow/{formatted_date}")

# COMMAND ----------

#add extra columns - processed_timestamp and file_name
from pyspark.sql.functions import current_timestamp, col, right
taxi_df = taxi_df.withColumn("processed_timestamp", current_timestamp()).withColumn("file_name", col("_metadata.file_name"))


# COMMAND ----------

#write to the table yellow_trips_raw -- APPEND MODE
taxi_df.write.mode("append").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")