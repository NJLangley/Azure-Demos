# Databricks notebook source
# MAGIC %md
# MAGIC # 5) Dynamic Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC > **Note:** Autocomplete for python does not work until you run a cell against a normal cluster. However DLT notebooks cannot be run on a normal cluster, so run the single cell below to get autocomplete (somewhat) working 

# COMMAND ----------

print('Autocomplete should work now')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import python modules

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create tables

# COMMAND ----------

landed_root_path = 'abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/landed/csv'
lake_root_path = 'abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/Demo2'

# COMMAND ----------

tables_to_load = [
  'Address',
  'Customer',
  'CustomerAddress',
  'Product',
  'ProductCategory',
  'ProductDescription',
  'ProductModel',
  'ProductModelProductDescription',
  'SalesOrderDetail',
  'SalesOrderHeader'
]

format_options = {
  "cloudFiles.format": "csv",
  "delimiter": "|",
  "header": "true",
  "inferSchema": "true"
}

# COMMAND ----------

# Define a function that sets the variables based on the current table, then defines a function as a DLT table for the current table
# This approch ensures that the functions creating the dlt tables are in different scopes, so they can all have the same name
def ingest_to_bronze(table):
  bronze_root_path = f'{lake_root_path}/bronze'
  landed_path = f'{landed_root_path}/{table}'
  bronze_table = f'Bronze_{table}'
  bronze_table_path = f'{bronze_root_path}/{table}'
  
  @dlt.table(
    name=bronze_table,
    comment=f'The Adventure Works bronze {table} table ingested from landed data',
    # We have to tell it where to put the files, otherwise they go in the Storeage Location defined in the DLT job
    path=bronze_table_path 
  )
  def ingest_data():
    df = (spark.readStream.format("cloudFiles")
               .options(**format_options)
               .load(landed_path)
               .withColumn("source_filename", input_file_name())
         )
    return df

# COMMAND ----------

for table in tables_to_load:
  ingest_to_bronze(table)
