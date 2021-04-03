package com.mycompany.data.pipeline

import com.confix.{PipelineConfV1}
import org.apache.spark.sql.SparkSession

object PipelineMain {
  def main(args: Array[String]): Unit = {
    implicit val env: String = "dev"
    val pipelineConf: PipelineConfV1 = PipelineConfV1.loadFromResource("ingest_stream_rate_sample.conf")
    val dbSpark: SparkSession = SparkSession.builder().getOrCreate()
    new IngestStreamRateSample(dbSpark, pipelineConf).run
  }
}
