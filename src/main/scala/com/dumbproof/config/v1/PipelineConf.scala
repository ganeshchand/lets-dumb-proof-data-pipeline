package com.dumbproof.config.v1

import com.dumbproof.config.ConfigLoader
import com.typesafe.config.{ConfigFactory, Config => TypeSafeConfig}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration => ScalaDuration}

case class PipelineConf(
                         triggerInterval: ScalaDuration,
                         checkpointPath: String,
                         outputPath: String,
                         rowsPerSecond: Int,
                         tuning: Seq[String]
                       ) {
  // user input validation
  require(rowsPerSecond > 0, s"input rate value must be greater than 0. Got $rowsPerSecond")

  override def toString: String =
    s"""
       |triggerInterval: $triggerInterval, checkpointPath: $checkpointPath, outputPath: $outputPath, rowsPerSecond: $rowsPerSecond
       |tuning: $tuning""".stripMargin
}

object PipelineConf {

  def loadFromFile(filePath: String, env: String): PipelineConf = {
    getPipelineConf(ConfigLoader.loadFromFile(filePath), env)
  }

  def loadFromResource(resourceName: String, env: String): PipelineConf = {
    getPipelineConf(ConfigLoader.loadFromResource(resourceName), env)
  }

  def loadFromString(filePath: String, env: String): PipelineConf = {
    getPipelineConf(ConfigLoader.loadFromString(filePath), env)
  }

  def loadFromDBFSFile(filePath: String, env: String): PipelineConf = {
    getPipelineConf(ConfigLoader.loadFromDBFSFile(filePath), env)
  }

  def getPipelineConf(typeSafeConfig: TypeSafeConfig, env: String): PipelineConf = {
    val config = typeSafeConfig.resolve()
    PipelineConf(
      triggerInterval = ScalaDuration.fromNanos(config.getDuration(s"$env.triggerInterval").toNanos),
      checkpointPath = config.getString(s"$env.checkpointPath"),
      outputPath = config.getString(s"$env.outputPath"),
      rowsPerSecond = config.getInt(s"$env.rowsPerSecond"),
      tuning = config.getStringList(s"$env.tuning").asScala
    )
  }
}

