package com.dumbproof.config.v3

import com.dumbproof.config.ConfigLoader
import com.typesafe.config.{ConfigFactory, Config => TypeSafeConfig}
import pureconfig._
import pureconfig.generic.auto._

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration => ScalaDuration}

case class PipelineConf(
                         triggerInterval: ScalaDuration,
                         checkpointPath: String,
                         outputPath: String,
                         rowsPerSecond: Int,
                         tuning: Seq[String]
                       ) {
  // validation here for instant feedback on change in user input
  require(rowsPerSecond > 0, s"input rate value must be greater than 0. Got $rowsPerSecond")

  override def toString: String =
    s"""
       |triggerInterval: $triggerInterval, checkpointPath: $checkpointPath, outputPath: $outputPath, rowsPerSecond: $rowsPerSecond
       |tuning: $tuning""".stripMargin
}

object PipelineConf {

  import pureconfig.generic.ProductHint

  implicit def hint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

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
    ConfigSource.fromConfig(config.getConfig(env)).loadOrThrow[PipelineConf]
  }
}

