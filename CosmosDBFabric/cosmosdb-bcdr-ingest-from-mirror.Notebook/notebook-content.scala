// Fabric notebook source

// METADATA ********************

// META {
// META   "kernel_info": {
// META     "name": "synapse_pyspark"
// META   },
// META   "dependencies": {
// META     "environment": {
// META       "environmentId": "35dff217-6416-b023-41e5-c4defe6dcfd4",
// META       "workspaceId": "00000000-0000-0000-0000-000000000000"
// META     }
// META   }
// META }

// CELL ********************

// MAGIC %%sql
// MAGIC CREATE TABLE IF NOT EXISTS cosmosCatalog.`CosmosDBDrill1105DB`.TaxiRecords
// MAGIC USING cosmos.oltp
// MAGIC TBLPROPERTIES(partitionKeyPath = '/id', autoScaleMaxThroughput = '10000', indexingPolicy = 'OnlySystemProperties', defaultTtlInSeconds = '108000000' );

// METADATA ********************

// META {
// META   "language": "sparksql",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));

val commonCfg = Map(
  "spark.cosmos.auth.type" -> "AccessToken",
  "spark.cosmos.accountEndpoint" -> "https://846f0501-2610-4b3a-80b4-baa8ced86271.z84.dxt-sql.cosmos.fabric.microsoft.com:443/",
  "spark.cosmos.accountDataResolverServiceName" -> "com.azure.cosmos.spark.fabric.FabricAccountDataResolver",
  "spark.cosmos.useGatewayMode" -> "true",
  "spark.cosmos.auth.aad.audience" -> "https://cosmos.azure.com/.default",
  "spark.cosmos.database" -> "CosmosDBDrill1105DB",
  "spark.cosmos.container" -> "TaxiRecords_Recovery",
  "spark.cosmos.read.consistencyStrategy" -> "LOCAL_COMMITTED",
  "spark.cosmos.diagnostics" -> "sampled"
)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

println(s"Creating sample records")

val sampleData = (1 to 100).map { i =>
  val name = s"Name_$i"
  val age = 20 + (i % 30) // age between 20 and 49
  val city = Seq("Seattle", "New York", "London", "Tokyo", "Sydney")(i % 5)
  (s"$i", name, age, city)
}

// Convert to DataFrame
val df = sampleData.toDF("id", "name", "age", "city")
df.persist()
display(df)



// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val writeCfg = commonCfg + (
    "spark.cosmos.write.strategy" -> "ItemOverwrite" // Upserting documents
    )

taxiRecordsRecoveryDF
.write
.format("cosmos.oltp")
.mode("Append")
.options(writeCfg)
.save()    

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************


val deltaPath = "abfss://CosmosDBDrill1105@dxt-onelake.dfs.fabric.microsoft.com/cosmosdb_bcdr_lakehouse.Lakehouse/Tables/TaxiRecords"
val taxiRecordsRecoveryDF = spark.read.format("delta").load(deltaPath)
taxiRecordsRecoveryDF.persist()


// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

// MAGIC %%sql
// MAGIC CREATE TABLE IF NOT EXISTS cosmosCatalog.`CosmosDBDrill1105DB`.TaxiRecords3
// MAGIC USING cosmos.oltp
// MAGIC TBLPROPERTIES(partitionKeyPath = '/id', autoScaleMaxThroughput = '10000', indexingPolicy = 'OnlySystemProperties', defaultTtlInSeconds = '108000000' );

// METADATA ********************

// META {
// META   "language": "sparksql",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val writeCfg = Map(
  "spark.cosmos.auth.type" -> "AccessToken",
  "spark.cosmos.accountEndpoint" -> "https://846f0501-2610-4b3a-80b4-baa8ced86271.z84.dxt-sql.cosmos.fabric.microsoft.com:443/",
  "spark.cosmos.accountDataResolverServiceName" -> "com.azure.cosmos.spark.fabric.FabricAccountDataResolver",
  "spark.cosmos.useGatewayMode" -> "true",
  "spark.cosmos.auth.aad.audience" -> "https://cosmos.azure.com/.default",
  "spark.cosmos.database" -> "CosmosDBDrill1105DB",
  "spark.cosmos.container" -> "TaxiRecords3",
  "spark.cosmos.read.consistencyStrategy" -> "LOCAL_COMMITTED",
  "spark.cosmos.diagnostics" -> "sampled",
  "spark.cosmos.write.strategy" -> "ItemOverwrite"
)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

taxiRecordsRecoveryDF
.write
.format("cosmos.oltp")
.mode("Append")
.options(writeCfg)
.save()   

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }
