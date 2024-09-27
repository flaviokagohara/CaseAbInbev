# Databricks notebook source
# MAGIC %md
# MAGIC Transforming data from silver layer into a gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold_layer.breweries_type_per_location as (
# MAGIC   select country, 
# MAGIC   city, 
# MAGIC   state,
# MAGIC   brewery_type,
# MAGIC   count(distinct id) as breweries_number
# MAGIC   from silver_layer.breweries 
# MAGIC   group by 1,2,3,4
# MAGIC   order by 5 desc
# MAGIC )

# COMMAND ----------


