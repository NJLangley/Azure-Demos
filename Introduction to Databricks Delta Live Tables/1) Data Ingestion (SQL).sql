-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Demo 1: Data Ingestion Using SQL
-- MAGIC ##### We can load data into delta live tables using either SQL or Python Notebook. This one is a SQL notebook.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > ***Note:*** *Update the paths for the data lake connection before running on your own environment!*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze / Ingestion tables

-- COMMAND ----------

CREATE STREAMING LIVE TABLE Bronze_Product
TBLPROPERTIES ("layer" = "bronze")
AS SELECT *,
          input_file_name() as FileName
   FROM cloud_files("abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/landed/csv/Product", 
                    "csv",
                    map(
                      "delimiter", "|",
                      "header", "true",
                      "inferSchema", "true"
                    )
                   );

-- COMMAND ----------

CREATE STREAMING LIVE TABLE Bronze_ProductCategory
COMMENT "The Adventure Works bronze product category table ingested from landed data"
TBLPROPERTIES ("layer" = "bronze")
AS SELECT *,
          input_file_name() as FileName
   FROM cloud_files("abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/landed/csv/ProductCategory", 
                    "csv",
                    map(
                      "delimiter", "|",
                      "header", "true",
                      "inferSchema", "true"
                    )
                   );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > ***Note:*** *The keyword* `INCREMENTAL` *does the same thing as* `STREAMING`*, but is and older syntax and is deprecated. Not all the docs are up to date though!*

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE Bronze_Customer
COMMENT "The Adventure Works bronze customer table ingested from landed data"
LOCATION "abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/Demo1/bronze/Customer"
TBLPROPERTIES ("layer" = "bronze")
AS SELECT *,
          input_file_name() as FileName
   FROM cloud_files("abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/landed/csv/Customer", 
                    "csv",
                    map(
                      "delimiter", "|",
                      "header", "true",
                      "pathGlobFilter", "Customer_[1-2].csv",
                      "inferSchema", "true"
                    )
                   );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### We can also create delta live views. These are not real views in the target DB, but more like intermediate stages in the ETL process.

-- COMMAND ----------

-- View's only exist as an intermediate step in a DLT pipeline. We cannot query them after the pipeline has run 
CREATE STREAMING LIVE VIEW Bronze_SalesOrderHeader
TBLPROPERTIES ("layer" = "bronze")
AS SELECT *,
          input_file_name() as FileName
   FROM cloud_files("abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/landed/csv/SalesOrderHeader", 
                    "csv",
                    map(
                      "delimiter", "|",
                      "header", "true",
                      "inferSchema", "true"
                    )
                   );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### We can also create a live table directly on a file using SQL syntax
-- MAGIC This table is not incremental, meaning it and it's dependencies are fully refreshed every run.
-- MAGIC 
-- MAGIC Unfortunately for CSV we can't use custom delimiters without creating a normal table first, and that is not supported in a DLT notebook. We can do this for parquet, delta and JSON data. The path must be surronded by backticks, not quotes.

-- COMMAND ----------

CREATE LIVE TABLE Bronze_ProductModel
COMMENT "The Adventure Works bronze product model table ingested from landed data"
LOCATION "abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/Demo1/bronze/ProductModel"
TBLPROPERTIES ("layer" = "bronze")
AS SELECT *,
          input_file_name() as FileName
   FROM parquet.`abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/landed/parquet/ProductModel/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver / Enriched tables
-- MAGIC ### Data Quality

-- COMMAND ----------

CREATE STREAMING LIVE TABLE Silver_SalesOrderHeader (
  CONSTRAINT Status_Not_Equal_To_99 EXPECT ( Status <> 99 ),
  CONSTRAINT Total_is_Correct EXPECT ( (SubTotal + TaxAmt + Freight) = TotalDue ) ON VIOLATION DROP ROW
)
COMMENT "The Adventure Works silver product table ingested from landed data"
LOCATION "abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/Demo1/silver/SalesOrderHeader"
TBLPROPERTIES ("layer" = "silver")
AS SELECT *
   FROM STREAM (live.Bronze_SalesOrderHeader)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The next table is not incremental, so is re-loaded each time the DLT workflow runs. It is reading from a incremental source, but as the target is not incremental we don't ad the `STREAM()` wrapper to the source table

-- COMMAND ----------

CREATE LIVE TABLE Silver_ProductCategory
COMMENT "The Adventure Works silver product category ingested from landed data"
LOCATION "abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/Demo1/silver/ProductCategory"
TBLPROPERTIES ("layer" = "silver")
AS SELECT *
   FROM live.Bronze_ProductCategory

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### We can join tables too
-- MAGIC There are some limits on join types though due to DLT being built on top of structured streaming

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > ***Note:*** *The query below does not work as a left join, as this would require a watermark on each side of the join to ensure late arriving data is handled correclty. Changing to an inner join works, but then the `ProductCategory_Is_Not_Null` data qaulity check is broken as records will get dropped in the join if the product category is not found.*

-- COMMAND ----------

-- This table is not incremental, so is fully loaded each time the pipeline runs
CREATE STREAMING LIVE TABLE Silver_Product_Enriched (
  CONSTRAINT SellStartDate_Is_Not_Null EXPECT (SellStartDate IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT ProductCategory_Is_Not_Null EXPECT (ProductCategory IS NOT NULL)
)
COMMENT "The Adventure Works silver product table ingested from landed data"
LOCATION "abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/Demo1/silver/Product"
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
--     ,pm.Name as ProductModel
   -- Join a live view and table together
   FROM STREAM(live.Bronze_Product) AS p
   -- Left joins are not supported for two live tables without adding a watermark in the join keys, or a watermark on the nullable side 
   -- and an appropriate range condition. This is a restriction inherited from spark structured streaming.
--    LEFT JOIN STREAM(live.Bronze_ProductCategory) AS pc
   -- Change to inner to make this run
   INNER JOIN STREAM(live.Bronze_ProductCategory) AS pc
     ON pc.ProductCategoryID = p.ProductCategoryID
--    LEFT JOIN live.Bronze_ProductModel AS pm
--      ON pm.ProductModelID = p.ProductModelID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > ***Note:*** *In the example above we can't join to the non-incremental table Silver_ProductCategory instead because we get a different error. In short the source(s) for an incremental table must be append only. The non-incremental source is replaced each time, and it knows that because the version counter on the Silver_ProductCategory delta table increments each time we re-run the DLT workflow*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CDC (Merge/Upsert)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Without using the CDC feature

-- COMMAND ----------

-- Without using the APPLY CHANGES INTO syntax, no deduplication happens
CREATE STREAMING LIVE TABLE Silver_Customer_No_SCD
COMMENT "The Adventure Works silver customer table using the SCD1 merge type"
LOCATION "abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/Demo1/silver/Customer_No_SCD"
TBLPROPERTIES ("layer" = "silver")
AS SELECT *
   FROM STREAM(live.Bronze_Customer);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### SCD1

-- COMMAND ----------

-- Incremental means the table is a streaming table, and we incrementlly add the changes to it
CREATE INCREMENTAL LIVE TABLE Silver_Customer_SCD1
COMMENT "The Adventure Works silver customer table using the SCD1 merge type"
LOCATION "abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/Demo1/silver/Customer_SCD1"
TBLPROPERTIES ("layer" = "silver");

-- COMMAND ----------

APPLY CHANGES INTO LIVE.Silver_Customer_SCD1
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
STORED AS SCD TYPE 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### SCD2

-- COMMAND ----------

-- Incremental means the table is a streaming table, and we incrementlly add the changes to it
CREATE STREAMING LIVE TABLE Silver_Customer_SCD2
COMMENT "The Adventure Works silver customer table using the SCD2 merge type"
LOCATION "abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/Demo1/silver/Customer_SCD2"
TBLPROPERTIES ("layer" = "silver");

-- COMMAND ----------

APPLY CHANGES INTO LIVE.Silver_Customer_SCD2
FROM STREAM(live.Bronze_Customer)
KEYS (CustomerID)
SEQUENCE BY ModifiedDate
COLUMNS * EXCEPT (PasswordHash, PasswordSalt, rowguid)
STORED AS SCD TYPE 2
