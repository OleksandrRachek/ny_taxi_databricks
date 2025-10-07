# Databricks notebook source
import urllib.request
import shutil
import os


# COMMAND ----------

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

response = urllib.request.urlopen(url)


dir_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup"

os.makedirs(dir_path, exist_ok=True)

    #with open(dir_path + '/yellow_tripdata_2025-01.parquet', 'wb')

local_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv"

with open(local_path, 'wb') as f:
    shutil.copyfileobj(response, f)