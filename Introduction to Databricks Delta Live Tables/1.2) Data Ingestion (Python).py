# Databricks notebook source
# MAGIC %md
# MAGIC # 1.2) Simple ingestion using Python
# MAGIC We load data into an incremental live table using either SQL or Python Notebook. This one is a Python notebook
# MAGIC 
# MAGIC Unlike normal notebooks we cannot 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import python modules
# MAGIC These are split so we don't import `dlt` when testing in the notebook mode
# MAGIC > **Note:** Autocomplete for python does not work until you run a cell against a normal cluster. However DLT notebooks cannot be run on a normal cluster, so run the single cell below to get autocomplete (somewhat) working 

# COMMAND ----------

import dlt

# COMMAND ----------

from pyspark.sql.functions import *
print('Autocomplete should work now')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create tables

# COMMAND ----------

@dlt.table(
  # We have to tell it where to put the files, otherwise they go in the Storeage Location defined in the DLT job
  path='abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/Demo1/bronze/Product'
)
def Bronze_Product():
  df = (spark.readStream.format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("delimiter", "|")
             .option("header", "true")
             .option("inferSchema", "true")
             .load("abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/landed/csv/Product")
        )
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Magic commands except for %pip are ignored in DLT notebooks, so this SQL to create the product model table will *NOT* get run

# COMMAND ----------

CREATE INCREMENTAL LIVE TABLE Bronze_Product_Model
COMMENT "The Adventure Works bronze product model table ingested from landed data"
LOCATION "abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/Demo1/bronze/Product_Model"
TBLPROPERTIES ("layer" = "bronze")
AS SELECT * 
   FROM cloud_files("abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/landed/csv/Product_Model", 
                    "csv",
                    map(
                      "delimiter", "|",
                      "header", "true",
                      "inferSchema", "true"
                    )
                   );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python allows more flexibility than SQL
# MAGIC We can refactor the code to put the storage loations, format info and table properties into variables which we can pass to a more generic function. This allows us to test the logic for paths more easily

# COMMAND ----------

landed_root_path = 'abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/landed/csv'
lake_root_path = 'abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/Demo1'

# COMMAND ----------

table = 'ProductCategory'
format_options = {
  "cloudFiles.format": "csv",
  "delimiter": "|",
  "header": "true",
  "inferSchema": "true"
}

# COMMAND ----------

# Derive actual paths
bronze_root_path = f'{lake_root_path}/bronze'
silver_root_path = f'{lake_root_path}/silver'
landed_path = f'{landed_root_path}/{table}'
bronze_table = f'Bronze_{table}'
bronze_table_path = f'{bronze_root_path}/{table}'

print(f'bronze_root_path:  {bronze_root_path}')
print(f'silver_root_path:  {silver_root_path}')
print(f'landed_path:       {landed_path}')
print(f'bronze_table:      {bronze_table}')
print(f'bronze_table_path: {bronze_table_path}')

# COMMAND ----------

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

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC https://rajanieshkaushikk.com/2022/06/24/why-the-databricks-delta-live-tables-are-the-next-big-thing/
# MAGIC 
# MAGIC https://rajanieshkaushikk.com/2022/07/06/how-to-implement-databricks-delta-live-tables-in-three-easy-steps/
# MAGIC 
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/workflows/delta-live-tables/delta-live-tables-data-sources
# MAGIC 
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/options#generic-options
# MAGIC 
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/workflows/delta-live-tables/delta-live-tables-cookbook
# MAGIC 
# MAGIC https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-python-ref.html
