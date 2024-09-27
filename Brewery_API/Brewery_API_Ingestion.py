# Databricks notebook source
# MAGIC %md
# MAGIC Importing libraries and setting up vars that is going to be used in the code

# COMMAND ----------

import requests
import json
import datetime

BREWERY_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

# COMMAND ----------

# MAGIC %md
# MAGIC Creating functions for API consulting

# COMMAND ----------

# function to read data from breweries metadata API
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

# MAGIC %md
# MAGIC Creating bronze layer in Volume

# COMMAND ----------

api_elements = check_count_elements()
print(f"'{api_elements}' elements was displayed in the API for today.")
extraction_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
breweries = json.dumps(api_consult(int(api_elements)))
dbutils.fs.put('/Volumes/flavioteste/bronze_layer/breweries_api/breweries_data.json', breweries, overwrite=True)
