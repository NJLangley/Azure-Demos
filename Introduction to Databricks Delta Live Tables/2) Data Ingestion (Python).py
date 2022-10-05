# Databricks notebook source
# MAGIC %md
# MAGIC > ***Note:*** *Update the paths for the data lake connection before running on your own environment!*

# COMMAND ----------

# MAGIC %md
# MAGIC # 2) Data ingestion Using Python
# MAGIC We load data into an incremental live table using either SQL or Python Notebook. This one is a Python notebook.
# MAGIC 
# MAGIC The Python notebook allows much more flexibilty in building delta live tables.
# MAGIC 
# MAGIC In python we define a delta live table using a function. This functions is decorated with one of the `@dlt` functions, making it a DLT table. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import python modules
# MAGIC These are split so we don't import `dlt` when testing in the notebook mode
# MAGIC > **Note:** Autocomplete for python does not work until you run a cell against a normal cluster. However DLT notebooks cannot be run on a normal cluster, so run the single cell below to get autocomplete (somewhat) working 

# COMMAND ----------

import dlt

# COMMAND ----------

from pyspark.sql.functions import *
print('Autocomplete should work now')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set the storage account and container in one place

# COMMAND ----------

storage_account = '<your storage account container>'
storage_container = '<your storage account>'

storage_root = f'abfss://{storage_container}@{storage_account}.dfs.core.windows.net/adventure-works'


# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 1: All the paths and delimiters are hardcoded just like the SQL examples.
# MAGIC We don't gain much from using Python in this example. We can't run the function to test it as it references the `dlt` module, so testing can be a pain. The function name defines the table name

# COMMAND ----------

@dlt.table(
  # We have to tell it where to put the files, otherwise they go in the Storeage Location defined in the DLT job
  path=f'{storage_root}/Demo2/bronze/Product'
)
def Bronze_Product():
    df = (spark.readStream.format("cloudFiles")
               .option('cloudFiles.format', 'csv')
               .option('delimiter', '|')
               .option('header', True)
               .option('inferSchema', True)
               .load(f'{storage_root}/landed/csv/Product')
          )
    return df

# COMMAND ----------

@dlt.table(
  # We have to tell it where to put the files, otherwise they go in the Storeage Location defined in the DLT job
  path=f'{storage_root}/Demo2/bronze/ProductCategory'
)
def Bronze_ProductCategory():
    df = (spark.readStream.format("cloudFiles")
             .option('cloudFiles.format', 'csv')
             .option('delimiter', '|')
             .option('header', True)
             .option('inferSchema', True)
             .load(f'{storage_root}/landed/csv/ProductCategory')
        )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC > ***Note:*** *In the two queries below, PySpark does not have the `live.` table prefix, but SQL does.*

# COMMAND ----------

@dlt.table(
  # We have to tell it where to put the files, otherwise they go in the Storeage Location defined in the DLT job
  path=f'{storage_root}/Demo2/silver/Silver_Product_Enriched'
)
@dlt.expect('ProductCategory_Is_Not_Null', 'ProductCategory IS NOT NULL')
@dlt.expect_or_drop('SellStartDate_Is_Not_Null', 'SellStartDate IS NOT NULL')
def Silver_Product_Enriched():
    df_product = dlt.read_stream('Bronze_Product')
    df_product_category = dlt.read_stream('Bronze_ProductCategory')

    df = (df_product.alias('p')
                    .join(df_product_category.alias('pc'), col('p.ProductCategoryID') == col('pc.ProductCategoryID'), "inner")
                    .withColumn('ProductCategory', col('pc.Name'))
                    .select('p.ProductID', 'p.Name', 'p.ProductNumber', 'p.Color', 
                            'p.StandardCost', 'p.ListPrice', 'p.Size', 'p.Weight', 
                            'p.SellStartDate', 'p.SellEndDate', 'p.DiscontinuedDate', 'ProductCategory'
                           )
         )
    return df

# COMMAND ----------

@dlt.table(
  # We have to tell it where to put the files, otherwise they go in the Storeage Location defined in the DLT job
  path=f'{storage_root}/Demo2/silver/Silver_Product_Enriched_spark_sql'
)
@dlt.expect('ProductCategory_Is_Not_Null', 'ProductCategory IS NOT NULL')
@dlt.expect_or_drop('SellStartDate_Is_Not_Null', 'SellStartDate IS NOT NULL')
def Silver_Product_Enriched_spark_sql():
    df = (spark.sql("""
                       SELECT p.ProductID
                             ,p.Name
                             ,p.ProductNumber
                             ,p.Color
                             ,p.StandardCost
                             ,p.ListPrice
                             ,p.Size
                             ,p.Weight
                             ,p.SellStartDate
                             ,p.SellEndDate
                             ,p.DiscontinuedDate
                             ,pc.Name as ProductCategory
                       FROM STREAM(live.Bronze_Product) AS p
                       INNER JOIN STREAM(live.Bronze_ProductCategory) AS pc
                         ON pc.ProductCategoryID = p.ProductCategoryID
                    """)
         )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC > ***Note:*** *Magic commands except for %pip are ignored in DLT notebooks, so this SQL to create the product model table will *NOT* get evaluated when the DLT graph is built*

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE STREAMING LIVE TABLE Bronze_Product_Model
# MAGIC TBLPROPERTIES ("layer" = "bronze")
# MAGIC AS SELECT *,
# MAGIC           input_file_name() as FileName
# MAGIC    FROM cloud_files("abfss://<your storage account>@<your storage account container>.dfs.core.windows.net/adventure-works/landed/csv/ProductModel", 
# MAGIC                     "csv",
# MAGIC                     map(
# MAGIC                       "delimiter", "|",
# MAGIC                       "header", "true",
# MAGIC                       "inferSchema", "true"
# MAGIC                     )
# MAGIC                    );

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 2: We can paramterise the table name, paths & expectations
# MAGIC We can refactor the code to put the storage loations, format info and table properties into variables which we can pass to a more generic function. This allows us to test the logic for paths more easily

# COMMAND ----------

landed_root_path = f'{storage_root}/landed/csv'
lake_root_path = f'{storage_root}/Demo2'

# COMMAND ----------

# These variables are global to all the tables in the pipeline
bronze_root_path = f'{lake_root_path}/bronze'
silver_root_path = f'{lake_root_path}/silver'


print(f'bronze_root_path:  {bronze_root_path}')
print(f'silver_root_path:  {silver_root_path}')

# COMMAND ----------

#  This is the table to load and the config for the CSV reader
table = 'ProductModel'
format_options = {
  "cloudFiles.format": "csv",
  "delimiter": "|",
  "header": "true",
  "inferSchema": "true"
}
expect_all = {'Name_Is_Not_Null': 'Name IS NOT NULL'}

# We can derive the table name, source & target lake paths from the varibles above
landed_path = f'{landed_root_path}/{table}'
bronze_table = f'Bronze_{table}'
bronze_table_path = f'{bronze_root_path}/{table}'

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
@dlt.expect_all(expect_all)
def ingest_data():
    df = (spark.readStream.format("cloudFiles")
             .options(**format_options)
             .load(landed_path)
             .withColumn("source_filename", input_file_name())
       )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC We have a nice generic table definition that uses the variables above. But we can't reuse it easily because we can't call it in a loop with the variables passed in as parameters
# MAGIC ```
# MAGIC ingest_data()    # How do we pass parameters?
# MAGIC ```
