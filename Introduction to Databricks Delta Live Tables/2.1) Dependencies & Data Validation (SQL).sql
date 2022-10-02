-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Load a silver table from the bronze table, and apply some data validation rules.

-- COMMAND ----------

-- Incremental means the table is a streaming table, and we incrementlly add the changes to it
CREATE INCREMENTAL LIVE TABLE Silver_Customer
COMMENT "The Adventure Works silver customer table ingested from landed data"
LOCATION "abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/Demo1/silver/Customer"
TBLPROPERTIES ("layer" = "silver")
AS SELECT *
  -- Beacuse the table is incremental, we must load from a stream of the source live table
  FROM STREAM(live.Bronze_Customer)

-- COMMAND ----------

-- This table is not incremental, so is fully loaded each time the pipeline runs
CREATE LIVE TABLE Silver_Product_Enriched (
  CONSTRAINT SellStartDate_Is_Not_Null EXPECT (SellStartDate IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The Adventure Works silver product table ingested from landed data"
LOCATION "abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/Demo1/silver/Product"
TBLPROPERTIES ("layer" = "silver")
AS SELECT 
     p.ProductID
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
   -- Because it is not a incremental table, we load from the source live table, and not a stream of the source live table
   FROM live.Bronze_Product AS p
   INNER JOIN live.Bronze_ProductCategory AS pc
     ON pc.ProductCategoryID = p.ProductCategoryID
   
        
