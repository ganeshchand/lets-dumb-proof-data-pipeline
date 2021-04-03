package com.mycompany.data.pipeline

import com.confix.{PipelineConfV1}
import org.apache.spark.sql.functions.{current_timestamp, lit, to_date}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


class IngestStreamRateSample (spark: SparkSession, pipelineConf: PipelineConfV1) {

  import spark.implicits._

  private val input: DataFrame = spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", pipelineConf.rowsPerSecond)
    .load()

  private def save(df: Dataset[Row], batchId: Long): Unit = {
    val output = df
      // add columns
      .withColumn("etl_batch_id", lit(batchId))
      .withColumn("etl_insert_timestamp", current_timestamp())
      .withColumn("event_date", to_date('timestamp))
      // rename columns
      .withColumnRenamed("timestamp", "event_timestamp")
      .withColumnRenamed("value", "id")
      // finalize output columns - projection and ordering
      .selectExpr("etl_batch_id", "etl_insert_timestamp", "event_timestamp", "event_date")

    output
      .write
      .format("delta")
      .option("mergeSchema", "true")
      .mode("append")
      .partitionBy("event_date")
      .option("path", pipelineConf.outputPath)
      .save()

  }

  private val streamWriter = input
    .writeStream
    .queryName("ingest_stream_rate_delta_sample")
    .option("checkpointLocation", pipelineConf.checkpointPath)
    .trigger(Trigger.ProcessingTime(pipelineConf.triggerInterval))
    .foreachBatch(save _)

  def run: Unit = {
    streamWriter.start()
  }

}
