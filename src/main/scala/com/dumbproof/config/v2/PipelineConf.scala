package com.dumbproof.config.v2

import com.dumbproof.config.ConfigLoader
import com.typesafe.config.{ConfigBeanFactory, Config => TypeSafeConfig}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration => ScalaDuration}
import java.time.{Duration => JavaDuration}
import scala.beans.BeanProperty

case class Common(
                   @BeanProperty var rootOutputPath: String,
                   @BeanProperty var tablePathName: String,
                   @BeanProperty var tuning: java.util.List[String]
                 ) {
  def this() = this("", "", Seq.empty[String].asJava)
}

case class PipelineConf(
                         @BeanProperty var checkpointPath: String,
                         @BeanProperty var outputPath: String,
                         @BeanProperty var rowsPerSecond: Int,
                         @BeanProperty var triggerInterval: JavaDuration,
                         @BeanProperty var tuning: java.util.List[String]
                       ) {
  def this() = this("", "", 0, null, Seq.empty[String].asJava)

  override def toString: String =
    s"""
       |triggerInterval: $triggerInterval, checkpointPath: $checkpointPath, outputPath: $outputPath, rowsPerSecond: $rowsPerSecond
       |tuning: ${tuning.asScala}""".stripMargin
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
    ConfigBeanFactory.create(config.getConfig(env), classOf[PipelineConf])
  }

  def javaToScalaDuration(duration: JavaDuration): ScalaDuration = {
    ScalaDuration.fromNanos(duration.toNanos)
  }
}

