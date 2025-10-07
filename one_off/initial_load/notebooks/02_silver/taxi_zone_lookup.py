# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

lookup_df = spark.read.csv('/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv', header=True)

# COMMAND ----------

lookup_df = lookup_df.withColumn("processed_timestamp", current_timestamp()).withColumn("file_name", col("_metadata.file_name"))

# COMMAND ----------

lookup_df = lookup_df.select(
    col("LocationID").cast(IntegerType()).alias("location_id"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone").alias("service_zone"),
    col("processed_timestamp").alias("effective_date"),
    col("file_name"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# COMMAND ----------

lookup_df.display()

# COMMAND ----------

lookup_df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")