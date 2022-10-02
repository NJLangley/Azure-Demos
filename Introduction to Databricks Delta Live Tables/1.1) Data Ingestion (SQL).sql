-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1.1) Data Ingestion (SQL)
-- MAGIC ### We can load data into an incremental live table using either SQL or Python Notebook. This one is a SQL notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Bronze / Ingestion tables

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE Bronze_Customer
COMMENT "The Adventure Works bronze customer table ingested from landed data"
LOCATION "abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/Demo1/bronze/Customer"
TBLPROPERTIES ("layer" = "bronze")
AS SELECT * 
   FROM cloud_files("abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/landed/csv/Customer", 
                    "csv",
                    map(
                      "delimiter", "|",
                      "header", "true",
--                       "pathGlobFilter", "Customer.csv",
                      "inferSchema", "true"
                    )
                   );

-- COMMAND ----------

CREATE INCREMENTAL LIVE VIEW Bronze_Product
COMMENT "The Adventure Works bronze product table ingested from landed data"
TBLPROPERTIES ("layer" = "bronze")
AS SELECT * 
   FROM cloud_files("abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/landed/csv/Product", 
                    "csv",
                    map(
                      "delimiter", "|",
                      "header", "true",
                      "pathGlobFilter", "Product.csv",
                      "inferSchema", "true"
                    )
                   );

-- COMMAND ----------

-- View's only exist as an intermediate step in a DLT pipeline. We cannot query them after the pipeline has run 
CREATE INCREMENTAL LIVE TABLE Bronze_ProductCategory
COMMENT "The Adventure Works bronze product category table ingested from landed data"
TBLPROPERTIES ("layer" = "bronze")
AS SELECT * 
   FROM cloud_files("abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/landed/csv/ProductCategory", 
                    "csv",
                    map(
                      "delimiter", "|",
                      "header", "true",
                      "pathGlobFilter", "ProductCategory.csv",
                      "inferSchema", "true"
                    )
                   );

-- COMMAND ----------

-- View's only exist as an intermediate step in a DLT pipeline. We cannot query them after the pipeline has run 
CREATE INCREMENTAL LIVE TABLE Bronze_SalesOrderHeader
COMMENT "The Adventure Works bronze sales order header table ingested from landed data"
TBLPROPERTIES ("layer" = "bronze")
AS SELECT * 
   FROM cloud_files("abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/landed/csv/SalesOrderHeader", 
                    "csv",
                    map(
                      "delimiter", "|",
                      "header", "true",
                      "pathGlobFilter", "SalesOrderHeader.csv",
                      "inferSchema", "true"
                    )
                   );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Silver / Enriched tables - Data Quality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > ***Note:*** *The query below does not work as a left join, as this would require a watermark on each side of the join to ensure late arriving data is handled correclty. Changing to an inner join works, but then the `ProductCategory_Is_Not_Null` data qaulity check is broken as records will get dropped in the join if the product category is not found.*

-- COMMAND ----------

-- This table is not incremental, so is fully loaded each time the pipeline runs
CREATE INCREMENTAL LIVE TABLE Silver_Product_Enriched (
  CONSTRAINT SellStartDate_Is_Not_Null EXPECT (SellStartDate IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT ProductCategory_Is_Not_Null EXPECT (ProductCategory IS NOT NULL)
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
   -- Join a live view and table together
   FROM STREAM(live.Bronze_Product) AS p
   -- Change to inner to make this run
   INNER JOIN STREAM(live.Bronze_ProductCategory) AS pc
     ON pc.ProductCategoryID = p.ProductCategoryID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Silver / Enriched tables - CDC (Merge/Upsert)

-- COMMAND ----------

-- Incremental means the table is a streaming table, and we incrementlly add the changes to it
CREATE INCREMENTAL LIVE TABLE Silver_Customer
COMMENT "The Adventure Works silver customer table ingested from landed data"
LOCATION "abfss://demo-lake@niallsdatalake.dfs.core.windows.net/adventure-works/Demo1/silver/Customer"
TBLPROPERTIES ("layer" = "silver");

-- COMMAND ----------

APPLY CHANGES INTO LIVE.Silver_Customer
FROM STREAM(live.Bronze_Customer)
KEYS (CustomerID)
-- [WHERE condition] -- Use for partition pruning
-- [IGNORE NULL UPDATES]
-- [APPLY AS DELETE WHEN condition]
-- [APPLY AS TRUNCATE WHEN condition]
SEQUENCE BY ModifiedDate
-- [COLUMNS {columnList | * EXCEPT (exceptColumnList)}]
COLUMNS * EXCEPT (PasswordHash, PasswordSalt, rowguid)
-- [STORED AS {SCD TYPE 1 | SCD TYPE 2}]
STORED AS SCD TYPE 2
