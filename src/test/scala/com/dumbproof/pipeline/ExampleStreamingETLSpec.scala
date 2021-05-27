package com.dumbproof.pipeline

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import com.dumbproof.config.v1. { PipelineConf => V1PipelineConf}

class ExampleStreamingETLSpec extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder()
    .master("local[4]")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
  spark.sparkContext.setLogLevel("error")
  val conf: V1PipelineConf = V1PipelineConf.loadFromResource("ingest_stream_rate_sample.conf", "dev")
  val etl = new ExampleStreamingETL(spark, conf)

  it must "set spark configurations" in {
    spark.conf.get("spark.sql.shuffle.partitions") mustBe 2
  }
}
