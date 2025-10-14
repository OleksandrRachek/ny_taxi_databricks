# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round, lit,col, mean,hour, date_format
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_green_yellow = spark.read.table("nyctaxi.02_silver.green_and_yellow_trips_enriched")
df_fhv = spark.read.table("nyctaxi.02_silver.fhv_trips_enriched")
df_hv_fhv = spark.read.table("nyctaxi.02_silver.hv_fhv_trips_enriched")


# COMMAND ----------

df_green_yellow = (
    df_green_yellow.select(
        df_green_yellow.pickup_datetime,
        df_green_yellow.pu_zone,
        df_green_yellow.do_zone,
        df_green_yellow.passenger_count,
        df_green_yellow.trip_distance,
        df_green_yellow.fare_amount,
        df_green_yellow.total_amount,
        df_green_yellow.taxi_type)
    
)

# COMMAND ----------

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

mean_for_fill =df_union.agg(mean("trip_distance").alias("trip_distance"), mean("fare_amount").alias("fare_amount"), mean("total_amount").alias("total_amount"))
mean_dict = mean_for_fill.first().asDict()

print (mean_dict)

# COMMAND ----------

df_union = df_union.fillna(mean_dict)

# COMMAND ----------

#aggregations for daily trip summary (any taxi type)
df = (
    df_union.
        # group records by calendar date
        groupBy(df_union.pickup_datetime.cast("date").alias("pickup_date") ). 
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

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary")

# COMMAND ----------

#aggregations for daily trip summary (by taxi type)
df = (
    df_union.
        # group records by calendar date
        groupBy(df_union.pickup_datetime.cast("date").alias("pickup_date"), df_union.taxi_type ). 
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

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary_per_taxi_type")

# COMMAND ----------

#aggregations for daily trip summary (by pu_zone)
df = (
    df_union.
        # group records by calendar date
        groupBy(df_union.pickup_datetime.cast("date").alias("pickup_date"), df_union.pu_zone ). 
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

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary_per_pu_zone")

# COMMAND ----------

#aggregations for hour and pu_zone
df = (
    df_union.
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

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.hourly_trip_summary_per_pu_zone")

# COMMAND ----------

#aggregations for do_zone
df = (
    df_union.
        # group records by calendar date
        groupBy(date_format("pickup_datetime", "yyyy-MM").alias("year_month"), hour(df_union.pickup_datetime).alias("pickup_hour"), df_union.do_zone ). 
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

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.hourly_trip_summary_per_do_zone")

# COMMAND ----------

#Fraud detection
df =df_union.filter ((col("trip_distance")<1) & (col("total_amount")>50))


# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_fraud_detection")
