-- Databricks notebook source
-- MAGIC %md
-- MAGIC > ***Note:*** *Update the paths for the data lake connection before running on your own environment!*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Customer SCD tables

-- COMMAND ----------

SELECT *
FROM adventure_works_demo_1.Bronze_Customer
WHERE CustomerId = 6
ORDER BY ModifiedDate

-- COMMAND ----------

SELECT *
FROM adventure_works_demo_1.Silver_Customer_No_SCD
WHERE CustomerId = 6

-- COMMAND ----------

SELECT *
FROM adventure_works_demo_1.Silver_Customer_SCD1
WHERE CustomerId = 6

-- COMMAND ----------

SELECT *
FROM adventure_works_demo_1.Silver_Customer_SCD2
WHERE CustomerId = 6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Views

-- COMMAND ----------

-- This does not exist as it was a view
SELECT *
FROM adventure_works_demo_1.Bronze_SalesOrderHeaderBronze_ProductModel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver tables

-- COMMAND ----------

SELECT *
FROM adventure_works_demo_1.Silver_SalesOrderHeader

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT Events

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW event_log_raw
AS
SELECT *
FROM DELTA.`abfss://demo-lake@puddle.dfs.core.windows.net/adventure-works/DLT-pipelines/adventure_works_demo_1/system/events`

-- COMMAND ----------

SELECT *
FROM event_log_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### This is a sample query from the Databricks docs to show the data quality from the last run. 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW latest_update_id
AS
SELECT origin.update_id
FROM event_log_raw
WHERE event_type = 'create_update' 
ORDER BY timestamp DESC
LIMIT 1

-- COMMAND ----------

SELECT update_id
FROM latest_update_id
LIMIT 1

-- COMMAND ----------

SELECT
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  SUM(row_expectations.passed_records) as passing_records,
  SUM(row_expectations.failed_records) as failing_records
FROM
  (
    SELECT
      explode(
        from_json(
          details :flow_progress :data_quality :expectations,
          "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
        )
      ) row_expectations
    FROM
      event_log_raw
    WHERE
      event_type = 'flow_progress'
      AND origin.update_id = (SELECT update_id FROM latest_update_id LIMIT 1)
  )
GROUP BY
  row_expectations.dataset,
  row_expectations.name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Here's a query to get the number of rows updated/dropped on each table in the last run

-- COMMAND ----------

SELECT
  flow_name as TableName,
  metrics.num_output_rows,
  dropped_records.dropped_records
FROM
  (
    SELECT
      origin.flow_name,
      from_json(
        details :flow_progress :metrics, "struct<num_output_rows :int>"
      ) as metrics,
      from_json(
        details :flow_progress :data_quality, "struct<dropped_records :int>"
      ) as dropped_records
    FROM
      event_log_raw
    WHERE
      event_type = 'flow_progress'
      AND origin.update_id = (SELECT update_id FROM latest_update_id LIMIT 1)
  )
WHERE metrics.num_output_rows IS NOT NULL
ORDER BY
   flow_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Changing the query to add more columns does not update old records without a reload

-- COMMAND ----------

select * from adventure_works_demo_1.Silver_Product_Enriched
