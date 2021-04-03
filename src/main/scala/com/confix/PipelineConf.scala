package com.confix

import com.typesafe.config.{ConfigFactory, Config => TypeSafeConfig}

import java.time.{Duration => JavaDuration}
import scala.concurrent.duration.Duration

case class PipelineConfV1(
                         triggerInterval: Duration,
                         checkpointPath: String,
                         outputPath: String,
                         rowsPerSecond: Int
                       ) {
  // validation here for instant feedback on change in user input
  require(rowsPerSecond.toInt > 0, s"input rate value must be greater than 0. Got $rowsPerSecond")

  override def toString: String =
    s"""
      |triggerInterval: $triggerInterval, checkpointPath: $checkpointPath, outputPath: $outputPath, rowsPerSecond: $rowsPerSecond""".stripMargin
}

object PipelineConfV1 {
  private def JavaToScalaDuration(duration: JavaDuration): Duration = {
    Duration.fromNanos(duration.toNanos)
  }

  def loadFromFile(path: String)(implicit env: String): PipelineConfV1 = {
    val configStr = scala.io.Source.fromFile(path).getLines().mkString("\n")
    loadFromString(configStr)
  }

  def loadFromResource(name: String)(implicit env: String): PipelineConfV1 = {
    val conf: TypeSafeConfig = ConfigFactory.parseResources(name)
    fromConfig(conf)
  }

  def loadFromString(confStr: String)(implicit env: String): PipelineConfV1 = {
    val conf: TypeSafeConfig = ConfigFactory.parseString(confStr)
    fromConfig(conf)
  }

  def fromConfig(config: TypeSafeConfig)(implicit env: String): PipelineConfV1 = {
    val conf = config.resolve()
    PipelineConfV1(
      triggerInterval = JavaToScalaDuration(conf.getDuration(s"mycompany.data.pipeline.conf.$env.triggerInterval")),
      checkpointPath = conf.getString(s"mycompany.data.pipeline.conf.$env.checkpointPath"),
      outputPath = conf.getString(s"mycompany.data.pipeline.conf.$env.outputPath"),
      rowsPerSecond = conf.getInt(s"mycompany.data.pipeline.conf.$env.rowsPerSecond"))
  }

  def loadWith(f: => PipelineConfV1): PipelineConfV1 = f
}

object PipelineConfV1Test extends App {
  implicit val env: String = "qa"
  val pipelineConf = PipelineConfV1.loadFromFile("src/main/resources/ingest_stream_rate_sample.conf")
  println(pipelineConf)
  println(s"triggers every ${pipelineConf.triggerInterval._1} ${pipelineConf.triggerInterval._2}")

}

