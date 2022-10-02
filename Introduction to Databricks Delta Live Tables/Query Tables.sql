-- Databricks notebook source
-- DROP DATABASE adventure_works_demo_1 CASCADE

-- COMMAND ----------

SELECT *
FROM adventure_works_demo_1.Bronze_Customer
WHERE CustomerId = 6
ORDER BY CustomerId, ModifiedDate

-- COMMAND ----------

SELECT *
FROM adventure_works_demo_1.Silver_Customer
WHERE CustomerId = 6
ORDER BY CustomerId

-- COMMAND ----------

-- This does not exist as it was a view
SELECT *
FROM adventure_works_demo_1.Bronze_Product

-- COMMAND ----------

SELECT *
FROM adventure_works_demo_1.Bronze_ProductCategory

-- COMMAND ----------

SELECT *
FROM adventure_works_demo_1.Silver_Product_Enriched

-- COMMAND ----------

select *
from DELTA(path 'abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/DLT-pipelines/adventure_works_demo_1/system/events')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df_queue = spark.read.format('delta').load('abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/DLT-pipelines/adventure_works_demo_1/system/events')
-- MAGIC display(df_queue)
