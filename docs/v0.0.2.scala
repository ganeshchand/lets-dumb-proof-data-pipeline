// Databricks notebook source
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.Trigger.{Once, ProcessingTime}
import scala.concurrent.duration.Duration
import scala.util.Try

// COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("rowsPerSecond", "1", "input rate (rows per second)")
dbutils.widgets.text("outputPath", "/tmp/deltalake/sample/1/rate", "output table path")
dbutils.widgets.text("checkpointLocation", "", "stream writer checkpoint path")
dbutils.widgets.text("triggerInterval", "30 seconds", "microbatch frequency")

// COMMAND ----------

val rowsPerSecond: String = dbutils.widgets.get("rowsPerSecond")
val outputPath: String = dbutils.widgets.get("outputPath")
val checkpointLocation: String = {
  val checkpointLocationInput = dbutils.widgets.get("checkpointLocation")
  if(checkpointLocationInput.isEmpty) s"$outputPath/_checkpoint"
  else checkpointLocationInput
}
val triggerIntervalInput: String = dbutils.widgets.get("triggerInterval")
val triggerInterval: Option[Duration] = Try(Duration(triggerIntervalInput)).toOption

// validation here for instant feedback on change in user input
require(triggerInterval.isDefined, s"unable to parse triggerInerval '$triggerIntervalInput' to Duration")
require(rowsPerSecond.toInt > 0, s"input rate value must be greater than 0. Got $rowsPerSecond")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Read

// COMMAND ----------

val input = spark
  .readStream
  .format("rate")
  .option("rowsPerSecond", rowsPerSecond)
  .load()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Transform

// COMMAND ----------

def save(input: Dataset[Row], batchId: Long): Unit = {
  
  val output = input  
    // add audit columns
    .withColumn("etl_insert_timestamp", current_timestamp)
    .withColumn("etl_batch_id", lit(batchId))
    .withColumn("event_date", to_date('timestamp))
   // rename columns
    .withColumnRenamed("timestamp", "event_timestamp")
    .withColumnRenamed("value", "id")
  // finalize output columns
    .selectExpr("etl_batch_id", "etl_insert_timestamp", "event_timestamp", "id", "event_date")
  
  output
   .write
   .format("delta")
   .option("mergeSchema", "true")
   .mode("append")
   .partitionBy("event_date")
   .option("path", outputPath)
  .save
  
}

// COMMAND ----------

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false") // if you make a typo in the config, you won't know. try with 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Write

// COMMAND ----------

val writer = input
  .writeStream
  .queryName("ingest_rateSource_into_delta")
  .option("checkpointLocation", checkpointLocation)
  .trigger(ProcessingTime(triggerInterval.get))
  .foreachBatch(save _)
  .start()
