# Databricks notebook source
# MAGIC %md
# MAGIC Adding Code to transform the data from bronze layer into a silver table

# COMMAND ----------

print("Retrieving bronze data from volume")
bronze_data = spark.read.json('/Volumes/flavioteste/bronze_layer/breweries_api/breweries_data.json')
print("Creating a staging table to assist the bronze to silver transformation.")
bronze_data.write.mode("overwrite").saveAsTable("silver_layer.staging_bts_breweries")

# COMMAND ----------

# MAGIC %md
# MAGIC Using SQL to transform the staging table to the "real" silver layer

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silver_layer.breweries 
# MAGIC using delta 
# MAGIC partitioned by (country)
# MAGIC as (
# MAGIC with bronze as (select distinct id,
# MAGIC brewery_type,
# MAGIC city,
# MAGIC country,
# MAGIC to_timestamp(extracted_at) as extracted_at,
# MAGIC latitude,
# MAGIC longitude,
# MAGIC name,
# MAGIC phone,
# MAGIC postal_code,
# MAGIC state,
# MAGIC street,
# MAGIC concat_ws(" ",coalesce(address_1,""), coalesce(address_2,""), coalesce(address_3,"")) as full_address,
# MAGIC website_url,
# MAGIC row_number() over (partition by id, country, city order by to_timestamp(extracted_at)) as rn
# MAGIC from silver_layer.staging_bts_breweries)
# MAGIC select * except(rn)
# MAGIC  from bronze
# MAGIC where rn = 1
# MAGIC )
