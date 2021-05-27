package com.dumbproof.config.v2

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.duration.Duration

class PipelineConfSpec extends AnyFlatSpec with Matchers {
  val env: String = "dev"
  def sharedAssertions(pipelineConf: PipelineConf) = {
    pipelineConf.rowsPerSecond mustBe 1
    pipelineConf.outputPath mustBe "/dumbproof/deltalake/dev/sample/1/rate"
    pipelineConf.checkpointPath mustBe s"${pipelineConf.outputPath}/_checkpoint"
    PipelineConf.javaToScalaDuration(pipelineConf.triggerInterval) mustBe Duration("30 seconds")
    pipelineConf.tuning.size mustBe 1
  }

  it must "be able to load configurations from HOCON String" in {
    val configStr  = scala.io.Source.fromFile("src/main/resources/ingest_stream_rate_sample.conf")
      .getLines()
      .mkString("\n")


    val pipelineConf = PipelineConf.loadFromString(configStr, env)
    sharedAssertions(pipelineConf)
  }

  it must "be able to load configurations from resources folder" in {
    val pipelineConf: PipelineConf = PipelineConf.loadFromResource("ingest_stream_rate_sample.conf", env)
    sharedAssertions(pipelineConf)
  }

  it must "be able to load configurations from local file path " in {
    val filePath = getClass.getResource("/ingest_stream_rate_sample.conf").getPath
    val pipelineConf: PipelineConf = PipelineConf.loadFromFile(filePath = filePath, env)
    sharedAssertions(pipelineConf)
  }

}

