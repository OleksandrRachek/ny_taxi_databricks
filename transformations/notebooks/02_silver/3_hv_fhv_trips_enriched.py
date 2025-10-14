# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from datetime import date
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = get_month_start_n_months_ago(2)

# COMMAND ----------

# Load cleansed yellow taxi trip data from the Silver layer
# and filter to only include trips with a pickup datetime
# later than the start date from two months ago
df_trips = spark.read.table("nyctaxi.02_silver.fhv_trips_cleansed").filter(f"pickup_datetime > '{two_months_ago_start}'")

# Load taxi zone lookup data from the Silver layer
df_zones = spark.read.table("nyctaxi.02_silver.taxi_zone_lookup")
#load vendors lookup
df_vendors = spark.read.table("nyctaxi.01_bronze.trip_record_user_guide")

# COMMAND ----------

df_trips = (
    df_trips.join(
        df_zones,
        df_trips.pu_location_id == df_zones.location_id,
        "left"
    )
    .join(
        df_vendors,
        df_trips.dispatching_base_num == df_vendors.License_Number,
        "left"
    )
    .select(
        df_vendors.Base_Name.alias("dispatching_base_name"),
        df_vendors.App_Company_Affiliation.alias("dispatching_company_affiliation"),        
        df_trips.pickup_datetime,
        df_trips.dropOff_datetime,
        df_trips.trip_duration,
        df_zones.borough.alias("pu_borough"),
        df_zones.zone.alias("pu_zone"),
        df_trips.pu_location_id,
        df_trips.do_location_id,
        df_trips.sr_flag,
        df_trips.affiliated_base_number,
        df_trips.passenger_count,
        df_trips.taxi_type,
        df_trips.processed_timestamp,
        df_trips.source_file_name
    )
)

# COMMAND ----------

df_join_final =(
    df_trips.join(
        df_zones, 
        df_trips.do_location_id == df_zones.location_id,
        "left"
    )
    .join(
        df_vendors,
        df_trips.affiliated_base_number == df_vendors.License_Number,
        "left"
    )
    .select(
        df_trips.dispatching_base_name,
        df_trips.dispatching_company_affiliation,
        df_trips.pickup_datetime,
        df_trips.dropOff_datetime,
        df_trips.trip_duration,
        df_trips.pu_borough,
        df_trips.pu_zone,
        df_zones.borough.alias("do_borough"),
        df_zones.zone.alias("do_zone"),
        df_trips.sr_flag,
        df_vendors.Base_Name.alias("affiliated_base_name"),
        df_vendors.App_Company_Affiliation.alias("affiliated_company_affiliation"),
        df_trips.passenger_count,
        df_trips.taxi_type,
        df_trips.processed_timestamp,
        df_trips.source_file_name
    )
)

# COMMAND ----------

df_join_final.write.mode("append").saveAsTable("nyctaxi.02_silver.fhv_trips_enriched")
