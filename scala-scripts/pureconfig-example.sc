import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import pureconfig._
import pureconfig.generic.auto._

import scala.concurrent.duration.{Duration => ScalaDuration}

case class PipelineConf(
                         checkpointPath: String,
                         outputPath: String,
                         rowsPerSecond: Int,
                         triggerInterval: ScalaDuration,
                         tuning: Seq[String]
                       )

val configStr =
  """
    |common {
    |  rootOutputPath: /tmp/deltalake
    |  tablePathName: /sample/1/rate
    |  tuning: ["spark.sql.shuffle.partitions=2"]
    |}
    |
    |dev {
    |  outputPath: ${common.rootOutputPath}/dev${common.tablePathName}
    |  checkpointPath: ${dev.outputPath}/_checkpoint
    |  triggerInterval: 30s
    |  rowsPerSecond: 1
    |  tuning: ${common.tuning}
    |}
    |qa {
    |  outputPath: ${common.rootOutputPath}/qa${common.tablePathName}
    |  checkpointPath: ${qa.outputPath}/_checkpoint
    |  triggerInterval: 1m
    |  rowsPerSecond: 1000
    |  tuning: ${common.tuning}
    |}
    |prod {
    |  outputPath: ${common.rootOutputPath}/prod${common.tablePathName}
    |  checkpointPath: ${prod.outputPath}/_checkpoint
    |  triggerInterval: ${qa.triggerInterval}
    |  rowsPerSecond: ${qa.rowsPerSecond}
    |  tuning: ${common.tuning}
    |}""".stripMargin


import pureconfig.generic.ProductHint
implicit def hint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

val conf = ConfigFactory.parseString(configStr).resolve.getConfig("dev")
val pipelineConf = ConfigSource.fromConfig(conf).load[PipelineConf]
pipelineConf.isRight
pipelineConf.right.get


// Sealed trait

import scala.concurrent.duration.Duration

sealed trait Trigger

case object Once extends Trigger

case class TriggerDuration(duration: Duration) extends Trigger

case class PipelineConfig(trigger: Trigger)

//val confStr = """trigger { type=once }"""
val confStr = """trigger{type=trigger-duration, duration=1m}"""

val pipelineConfig = ConfigSource.string(confStr).loadOrThrow[PipelineConfig]

val pipelineTrigger: org.apache.spark.sql.streaming.Trigger = pipelineConfig.trigger match {
  case Once => Trigger.Once()
  case TriggerDuration(duration) => Trigger.ProcessingTime(duration)
}
