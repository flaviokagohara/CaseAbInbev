# Databricks notebook source
# MAGIC %md
# MAGIC Importing and creating variables

# COMMAND ----------

import requests
import json
import datetime

BREWERY_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

# COMMAND ----------

# MAGIC %md
# MAGIC # Unit test for check_count_elements function

# COMMAND ----------

def check_count_elements():
    elements_per_page = 50 
    count_params = {"page": 0,
                    "per_page":elements_per_page}
    
    reponse_metadata = requests.get(url = BREWERY_BASE_URL+"/meta", params = count_params)
    if(reponse_metadata.status_code) == 200:
        return reponse_metadata.json()['total']
    else:
        print("We could not retrieve data from the 'brewery_metadata' API.")
        reponse_metadata.raise_for_status()

# COMMAND ----------

#test 
test_check_count = check_count_elements()
print(test_check_count)

# COMMAND ----------

# MAGIC %md
# MAGIC # Unit test for retrieving data from breweries API

# COMMAND ----------

# function to extract data from breweries API
def api_consult(elements: int):
    brewery_data = []
    extraction_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elements_per_page = 200
    limit = elements // elements_per_page
    if elements%200 != 0:
        limit+=1
    print(f"We will retrieve data from '{limit}' pages, getting '{elements_per_page}' elements per page.")
    page_params = {"page": 0,
                   "per_page": elements_per_page}
    for page in range(limit+1):
        print(f"Extracting data for page: {page}")
        page_params['page'] = page
        extraction = requests.get(url = BREWERY_BASE_URL, params = page_params)
        if extraction.status_code == 200:
            data = extraction.json()
            for element in data:
                element['extracted_at'] = extraction_time
                if element not in brewery_data:
                    brewery_data.append(element)
        else: 
            print("We could not retrieve data from the 'brewery' API.")
            extraction.raise_for_status()
    return brewery_data

# COMMAND ----------

# testing
teste_api = api_consult(1000)
print(teste_api)

# COMMAND ----------

# MAGIC %md
# MAGIC Doing some quality checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- checking duplicate data 
# MAGIC select id, 
# MAGIC count(*)
# MAGIC from silver_layer.breweries
# MAGIC group by 1 
# MAGIC order by 2 desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- checking null values for locations 
# MAGIC select id
# MAGIC from silver_layer.breweries
# MAGIC where (country is null 
# MAGIC or city is null
# MAGIC or state is null)

# COMMAND ----------

# MAGIC %sql
# MAGIC --check possible way to fill in null values
# MAGIC select distinct postal_code,
# MAGIC country, 
# MAGIC id, 
# MAGIC latitude, 
# MAGIC longitude
# MAGIC from silver_layer.breweries
# MAGIC where (latitude is null
# MAGIC and longitude is null)
# MAGIC and postal_code is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC -- checking different columns with same data
# MAGIC select state,
# MAGIC state_province
# MAGIC from silver_layer.staging_bts_breweries
# MAGIC where state != state_province

# COMMAND ----------

# MAGIC %sql
# MAGIC -- checking different columns with same data
# MAGIC select street,
# MAGIC address_1
# MAGIC from silver_layer.staging_bts_breweries
# MAGIC where street != address_1
