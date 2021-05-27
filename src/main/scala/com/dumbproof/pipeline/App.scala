package com.dumbproof.pipeline

import com.dumbproof.config.v1.{ PipelineConf => V1PipelineConf}
import com.dumbproof.config.v2.{ PipelineConf => V2PipelineConf}
import com.dumbproof.config.v3.{ PipelineConf => V3PipelineConf}

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val env = args(0)
    val spark = SparkSession.getActiveSession.get
    val pipelineConf: V1PipelineConf = V1PipelineConf.loadFromResource("ingest_stream_rate_sample.conf", env)
    new ExampleStreamingETL(spark, pipelineConf).run
  }
}
