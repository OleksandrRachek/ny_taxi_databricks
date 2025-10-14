# Databricks notebook source
#import DF's
df_yellow = spark.read.table("nyctaxi.02_silver.yellow_trips_cleansed")
df_green = spark.read.table("nyctaxi.02_silver.green_trips_cleansed")

# COMMAND ----------

#select columns for green taxi
df_green = df_green.select ('vendor', 'pickup_datetime', 'dropoff_datetime', 'trip_duration', 'passenger_count', 'trip_distance', 'rate_type', 'store_and_fwd_flag', 'pu_location_id', 'do_location_id', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee', 'trip_type', 'taxi_type', 'processed_timestamp', 'source_file_name')

# COMMAND ----------

#select columns for yellow taxi
df_yellow = df_yellow.select ('vendor', 'pickup_datetime', 'dropoff_datetime', 'trip_duration', 'passenger_count', 'trip_distance', 'rate_type', 'store_and_fwd_flag', 'pu_location_id', 'do_location_id', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee', 'trip_type', 'taxi_type', 'processed_timestamp', 'source_file_name')


# COMMAND ----------

union_df = df_yellow.union(df_green)

# COMMAND ----------

union_df.write.mode("append").saveAsTable("nyctaxi.02_silver.green_and_yellow_trips_cleansed")

# COMMAND ----------


