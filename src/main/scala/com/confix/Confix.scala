package com.confix

import com.typesafe.config.{ConfigBeanFactory, ConfigFactory}

import scala.beans.BeanProperty

case class Common(
                   @BeanProperty var rootOutputPath: String,
                   @BeanProperty var tablePathName: String
                 ) {
  def this() = this("", "")
}

case class Conf(

                 @BeanProperty var common: Common,
                 @BeanProperty var dev: PipelineConf,
                 @BeanProperty var prod: PipelineConf,
                 @BeanProperty var qa: PipelineConf
               ) {
  def this() = this(null, null, null, null)
}

case class PipelineConf(
                         @BeanProperty var checkpointPath: String,
                         @BeanProperty var outputPath: String,
                         @BeanProperty var rowsPerSecond: Int,
                         @BeanProperty var triggerInterval: String
                       ) {
  def this() = this("", "", 0, "")
}

case class Pipeline(
                     @BeanProperty var conf: Conf
                   ) {
  //needed by configfactory to conform to java bean standard
  def this() = this(null)
}

object Test extends App {

  val configStr  = """mycompany.data.pipeline.conf {
  common {
    rootOutputPath: /tmp/deltalake
    tablePathName: /sample/1/rate
  }
  dev {
    outputPath: ${pipeline.conf.common.rootOutputPath}/dev${pipeline.conf.common.tablePathName}
    checkpointPath: ${pipeline.conf.dev.outputPath}/_checkpoint
    triggerInterval: 30s
    rowsPerSecond: 1
  }
  qa {
    outputPath: ${pipeline.conf.common.rootOutputPath}/qa${pipeline.conf.common.tablePathName}
    checkpointPath: ${pipeline.conf.qa.outputPath}/_checkpoint
    triggerInterval: 1m
    rowsPerSecond: 1000
  }
  prod {
    outputPath: ${pipeline.conf.common.rootOutputPath}/prod${pipeline.conf.common.tablePathName}
    checkpointPath: ${pipeline.conf.prod.outputPath}/_checkpoint
    triggerInterval: ${pipeline.conf.qa.triggerInterval}
    rowsPerSecond: ${pipeline.conf.qa.rowsPerSecond}
  }
}"""

  val pipelineConfig = ConfigFactory.parseString(configStr).resolve()

  val pipelineConfDev = ConfigBeanFactory.create(pipelineConfig.getConfig("pipeline.conf.dev"), classOf[PipelineConf])
  println(pipelineConfDev)
  val pipelineConfCommon: Common = ConfigBeanFactory.create(pipelineConfig.getConfig("pipeline.conf.common"), classOf[Common])
  println(pipelineConfCommon)

  println(pipelineConfig.getConfig("pipeline"))
  val pipelineAllConf: Pipeline =  ConfigBeanFactory.create(pipelineConfig.getConfig("pipeline"), classOf[Pipeline])
  println(pipelineAllConf)
}