import com.typesafe.config.ConfigBeanFactory

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

object v1 {

  // imports

  import com.typesafe.config.ConfigFactory
  import com.typesafe.config.{Config => TypesafeConfig}
  import scala.concurrent.duration.{Duration => ScalaDuration}
  import collection.JavaConverters._

  // shape of your config
  case class PipelineConf(
                           checkpointPath: String,
                           outputPath: String,
                           rowsPerSecond: Int,
                           triggerInterval: ScalaDuration,
                           tuning: Seq[String]
                         )


  val env: String = "dev"
  lazy val pipelineConfig: TypesafeConfig = ConfigFactory.parseString(configStr).resolve()
  v1.pipelineConfig.getString(s"$env.checkpointPath")
  v1.pipelineConfig.getString(s"$env.outputPath")
  v1.pipelineConfig.getString(s"$env.rowsPerSecond")
  v1.pipelineConfig.getStringList(s"$env.tuning").asScala

  lazy val devPipelineConfig = PipelineConf(
    triggerInterval = ScalaDuration.fromNanos(pipelineConfig.getDuration(s"$env.triggerInterval").toNanos),
    checkpointPath = pipelineConfig.getString(s"$env.checkpointPath"),
    outputPath = pipelineConfig.getString(s"$env.outputPath"),
    rowsPerSecond = pipelineConfig.getInt(s"$env.rowsPerSecond"),
    tuning = pipelineConfig.getStringList(s"$env.tuning").asScala
  )
}

// V1: test
v1.devPipelineConfig.tuning

/*
 * You can set spark configs using spark.sql("SET key=value") and assert
  v1.devPipelineConfig.tuning.foreach(config => spark.sql(s"SET $config"))
  assert(spark.conf.get("spark.sql.shuffle.partitions") == "2")
 */


object v2 {

  // imports

  import com.typesafe.config.ConfigFactory
  import com.typesafe.config.{Config => TypesafeConfig}
  import scala.concurrent.duration.{Duration => ScalaDuration}
  import collection.JavaConverters._
  // additional imports
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
                           @BeanProperty var triggerInterval: java.time.Duration,
                           @BeanProperty var tuning: java.util.List[String]
                         ) {
    def this() = this("", "", 0, null, Seq.empty[String].asJava)
  }

  val env: String = "dev"
  lazy val pipelineConfig: TypesafeConfig = ConfigFactory.parseString(configStr).resolve() // load config
  lazy val devPipelineConfig = ConfigBeanFactory.create(pipelineConfig.getConfig(env), classOf[PipelineConf])
}

v2.devPipelineConfig.tuning // java.util.List
import collection.JavaConverters._
v2.devPipelineConfig.tuning.asScala.foreach(println)
v2.devPipelineConfig.triggerInterval

/*
 * Requires java.util.List to Scala collection conversion
   import collection.JavaConverters._
   // You can set spark configs using spark.sql("SET key=value") and assert
  v2.devPipelineConfig.tuning.asScala.foreach(config => spark.sql(s"SET $config"))
  assert(spark.conf.get("spark.sql.shuffle.partitions") == "2")
 */
