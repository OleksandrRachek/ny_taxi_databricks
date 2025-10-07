# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col,right


# COMMAND ----------

taxi_df = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/nytaxi_yellow/*")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, right
taxi_df = taxi_df.withColumn("processed_timestamp", current_timestamp()).withColumn("file_name", col("_metadata.file_name"))


# COMMAND ----------

taxi_df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")