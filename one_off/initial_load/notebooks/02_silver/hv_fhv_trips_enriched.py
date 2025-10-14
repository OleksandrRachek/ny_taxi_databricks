# Databricks notebook source
df_trips = spark.read.table("nyctaxi.02_silver.hv_fhv_trips_cleansed")
df_zones = spark.read.table("nyctaxi.02_silver.taxi_zone_lookup")
df_vendors = spark.read.table("nyctaxi.01_bronze.trip_record_user_guide")

# COMMAND ----------

df_trips = (
    df_trips.join(
        df_zones, 
        df_trips.pu_location_id == df_zones.location_id,
        "left"
    )
    .join(df_vendors, 
        df_trips.dispatching_base_num == df_vendors.License_Number,
        "left")
    .select(
    df_vendors.Base_Name.alias("dispatching_base_name"),
    df_vendors.App_Company_Affiliation.alias("dispatching_company_affiliation"),
    df_trips.originating_base_num,
    df_trips.request_datetime,
    df_trips.on_scene_datetime,
    df_trips.pickup_datetime,
    df_trips.dropoff_datetime,
    df_trips.trip_duration,
    df_zones.borough.alias("pu_borough"),   # pickup borough
    df_zones.zone.alias("pu_zone"),         # pickup zone,
    df_trips.pu_location_id,
    df_trips.do_location_id,
    df_trips.trip_miles,
    df_trips.trip_time,
    df_trips.base_passenger_fare,
    df_trips.tolls,
    df_trips.bcf,
    df_trips.sales_tax,
    df_trips.congestion_surcharge,
    df_trips.airport_fee,
    df_trips.tips,
    df_trips.driver_pay,
    df_trips.shared_request_flag,
    df_trips.shared_match_flag,
    df_trips.access_a_ride_flag,
    df_trips.wav_request_flag,
    df_trips.wav_match_flag,
    df_trips.cbd_congestion_fee,
    df_trips.taxi_type,
    df_trips.processed_timestamp,
    df_trips.source_file_name
    )
)




# COMMAND ----------

df_join_final = (
    df_trips.join(
        df_zones, 
        df_trips.do_location_id == df_zones.location_id,
        "left"
        )
    .join(df_vendors, 
        df_trips.originating_base_num == df_vendors.License_Number,
        "left")
    .select(
        df_trips.dispatching_base_name,
        df_trips.dispatching_company_affiliation,
        df_trips.originating_base_num,
        df_trips.request_datetime,
        df_trips.on_scene_datetime,
        df_trips.pickup_datetime,
        df_trips.dropoff_datetime,
        df_trips.trip_duration,
        df_trips.pu_borough,   # pickup borough
        df_trips.pu_zone,         # pickup zone,
        df_zones.borough.alias("do_borough"), # dropoff borough
        df_zones.zone.alias("do_zone"),
        df_trips.trip_miles,
        df_trips.trip_time,
        df_trips.base_passenger_fare,
        df_trips.tolls,
        df_trips.bcf,
        df_trips.sales_tax,
        df_trips.congestion_surcharge,
        df_trips.airport_fee,
        df_trips.tips,
        df_trips.driver_pay,
        df_trips.shared_request_flag,
        df_trips.shared_match_flag,
        df_trips.access_a_ride_flag,
        df_trips.wav_request_flag,
        df_trips.wav_match_flag,
        df_trips.cbd_congestion_fee,
        df_trips.taxi_type,
        df_trips.processed_timestamp,
        df_trips.source_file_name
    )
)




# COMMAND ----------

df_join_final.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.hv_fhv_trips_enriched")
