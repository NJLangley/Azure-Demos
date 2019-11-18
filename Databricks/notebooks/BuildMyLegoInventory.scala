// Databricks notebook source
// MAGIC %md
// MAGIC #Demo to show how we read and write using Databricks & Scala
// MAGIC 
// MAGIC We can read and write write data from amy sources using Databricks. This demo show how we can use a Scala to read data from Azure Data Lake, transform it, and write it back out into another layer in the data lake.
// MAGIC 
// MAGIC The demo uses data about Lego parts and sets freely avalibale for free from [Rebrickable](https://rebrickable.com/downloads/). The shema of the files looks like this.
// MAGIC 
// MAGIC <img src="https://rebrickable.com/static/img/diagrams/downloads_schema_v2.png" />

// COMMAND ----------

// MAGIC %md
// MAGIC ##Configure any variable we want to reuse throughout the notebook

// COMMAND ----------

val clientId: String = "378bf65d-7d6e-4dd1-8abf-fc8c1548b211"
val keyScope: String = "demo-secrets"
val keyNameServicePrincipalSecret: String = "DataBricksServiceCredential"
val directoryId: String = "535326ca-52b6-4c57-b6dc-e017f69faf50"
val storageAccountName: String = "niallsdatalake"
val fileSystemName: String = "demo-lake"
val sourceRoot: String = s"abfss://$fileSystemName@$storageAccountName.dfs.core.windows.net"
val keyNameStorageAccountAccessKey: String = "DataLakeStorageAccountAccessKey"


// COMMAND ----------

// MAGIC %md
// MAGIC ##Configure the credentials and mount the root directory
// MAGIC Direct access using OAuth2 has not been working for me, but the mount points are, so we're using those. Probably permissions related somewhere...

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> clientId,
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = keyScope, keyNameServicePrincipalSecret),
  "fs.azure.account.oauth2.client.endpoint" -> s"https://login.microsoftonline.com/$directoryId/oauth2/token")

// Mount the storage if it is not already mounted
val mountPoint = s"/mnt/$fileSystemName"
if (dbutils.fs.mounts.map(_.mountPoint).filter(_ == mountPoint).length == 0) {
 dbutils.fs.mount(
   source = sourceRoot,
   mountPoint = mountPoint,
   extraConfigs = configs)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##Read in the data into data frames

// COMMAND ----------

val mySets = spark.read
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv("/mnt/demo-lake/raw/rebrickable/csv/my_sets.csv")

mySets.createOrReplaceTempView("mySets")

val inventories = spark.read
                       .option("header", "true")
                       .option("inferSchema", "true")
                       .csv("/mnt/demo-lake/raw/rebrickable/csv/inventories.csv")
inventories.createOrReplaceTempView("inventories")

val inventoryParts = spark.read
                          .option("header", "true")
                          .option("inferSchema", "true")
                          .csv("/mnt/demo-lake/raw/rebrickable/csv/inventory_parts.csv")
inventoryParts.createOrReplaceTempView("inventoryParts")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Have a look at the data to see how it looks...

// COMMAND ----------

display(mySets)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Query the data using Scala

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ##We can also do the same query using SQL

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TEMPORARY VIEW vw_my_sets_parts
// MAGIC AS
// MAGIC SELECT m.*
// MAGIC       ,ip.*
// MAGIC FROM mySets AS m
// MAGIC INNER JOIN ( SELECT *
// MAGIC                     -- Latest version only
// MAGIC                    ,row_number() OVER (PARTITION BY set_num ORDER BY version DESC) AS row_num
// MAGIC              FROM inventories
// MAGIC            )AS i
// MAGIC   ON i.set_num = m.set_num
// MAGIC      AND i.row_num = 1
// MAGIC INNER JOIN inventoryParts AS ip
// MAGIC   ON ip.inventory_id = i.id;
// MAGIC 
// MAGIC SELECT *
// MAGIC FROM vw_my_sets_parts
// MAGIC LIMIT 100

// COMMAND ----------

// MAGIC %md
// MAGIC ##Now write the results from both queries back to data lake using Scala

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS default.my_sets_parts;
// MAGIC 
// MAGIC CREATE EXTERNAL TABLE my_sets_parts
// MAGIC   LOCATION '/mnt/demo-lake/processed/rebrickable/databricks/my_sets_parts'
// MAGIC   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
// MAGIC   STORED AS TEXTFILE
// MAGIC AS
// MAGIC SELECT *
// MAGIC FROM vw_my_sets_parts