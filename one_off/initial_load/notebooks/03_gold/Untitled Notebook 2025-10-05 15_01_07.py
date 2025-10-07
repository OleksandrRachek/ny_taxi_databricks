# Databricks notebook source
from pyspark.sql.functions import *

spark.read.table ('nyctaxi.02_silver.yellow_trips_cleansed').agg(max("tpep_pickup_datetime"), min("tpep_pickup_datetime")).display()
