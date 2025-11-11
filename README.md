# databricks_project
The project contains Databricks notedooks with transformations for NYC Taxi dataset 

There are few items that you need to make for run the project:
1 run ny_taxi_databricks/one_off/creating_catalogs_schemas_volume.py script


2 run one_off/initial_load/notebooks/01_bronze/parse_high_volume_license_number_lookup.ipynb notebook. Please check comments there 


3 Manually copy data into /Volumes/nyctaxi/00_landing/data_sources/.. from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page -- please see one_off/initial_load/notebooks/01_bronze notebooks


4 Run scripts from one_off/initial_load/notebooks/01_bronze, 02_silver, 03_gold folders 04_export is optional to simulate export during each runtime into other workload


5 For regular loads use transformations/notebooks


6 In the ad_hoc folder you can see purge scripts for data reloading, job_yml.yml contains yml definition for DataBricks job using transformation notebooks 


 
