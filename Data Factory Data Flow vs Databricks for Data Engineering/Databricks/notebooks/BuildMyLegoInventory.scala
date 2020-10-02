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
// MAGIC ##Import libraries we need

// COMMAND ----------

// Need this import for the agg() function in the last example
import org.apache.spark.SparkContext._

// COMMAND ----------

// MAGIC %md
// MAGIC ##Configure variables we want to reuse throughout the notebook

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
// MAGIC ##Display the data so we can look at the structure

// COMMAND ----------

display(mySets)

// COMMAND ----------

// MAGIC %md
// MAGIC ##We can query the data using SQL, storing the result in a temporary view
// MAGIC We are using a view and not a table, as the table is visiable to all users of the cluster, and we just want a transient step in our processing. Createing a view (or table by default) does not cache the data to memory.

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
// MAGIC ##We will use this view for 2 queries, so explcitly cache it in memory
// MAGIC 
// MAGIC If we built a table in the above query, we could use the SQL syntax below to cache it
// MAGIC 
// MAGIC     CACHE TABLE CACHED_TABLE AS
// MAGIC     SELECT *
// MAGIC     FROM ...

// COMMAND ----------

sqlContext.cacheTable("default.my_sets_parts")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Write the results from the SQL query back to the lake using SQL

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

// COMMAND ----------

// MAGIC %md
// MAGIC ##Use Scala to group the results of the SQL query into parts, and write that to the lake too

// COMMAND ----------

// Need this import for the agg() function in the last example
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import sqlContext.implicits._

// Define a Scala UDF
//val spareQuantitiy = udf( (isSpare: String,  quantity: Int) => if (isSpare == "t") {quantity} else {0} )




def getSpareQuantity = (isSpare: String,  quantity: Int) => {
  if (isSpare == "t") {
    quantity
  } else {
    0
  }
}

val spareQuantitiy = spark.udf.register("spareQuantitiy",getSpareQuantity)

val mySetParts = sqlContext.table("vw_my_sets_parts")
val myParts = mySetParts.select(col("part_num") as "part_num",
                                col("color_id") as "color_id",
                                col("quantity") as "quantity",
                                spareQuantitiy(col("is_spare"), col("quantity")) as "spare_quantity")
                        .groupBy("part_num", "color_id")
                        .agg(sum("quantity"),sum("spare_quantity"))
                        
// This sets the partitoning before we write the data
myParts.repartition(col("color_id"))
       .write
       // This writes the partitions to folders using hive syntax. Without the repartition we get about 1 row per file
       .partitionBy("color_id")
       .mode("overwrite")
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .save("/mnt/demo-lake/processed/rebrickable/databricks/my_parts")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Finally lets have a look at the data frame we wrote to disk to see some graphs

// COMMAND ----------

display(myParts)

// COMMAND ----------

display( myParts.repartition(col("color_id")) )