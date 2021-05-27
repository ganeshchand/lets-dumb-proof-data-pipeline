package com.dumbproof.pipeline

import com.dumbproof.config.v1.PipelineConf
import org.apache.spark.sql.functions.{current_timestamp, to_date}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class ExampleStreamingETL(spark: SparkSession, conf: PipelineConf) {

  import spark.implicits._

  conf.tuning.foreach { conf =>
    spark.sql(s"SET $conf")
  }

  // Read
  private val input: DataFrame = spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", conf.rowsPerSecond)
    .load()

  // Transform
  val output: DataFrame = input
    // add audit columns
    .withColumn("etl_insert_timestamp", current_timestamp)
    .withColumn("event_date", to_date('timestamp))
    // rename columns
    .withColumnRenamed("timestamp", "event_timestamp")
    .withColumnRenamed("value", "id")
    // finalize output columns
    .selectExpr("etl_insert_timestamp", "event_timestamp", "id", "event_date")

  private val streamWriter: DataStreamWriter[Row] = output
    .writeStream
    .format("delta")
    .queryName("ingest_rateSource_into_delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", conf.checkpointPath)
    .partitionBy("event_date")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(conf.triggerInterval))


  def run: Unit = {
    streamWriter.start(conf.outputPath)
  }
}
