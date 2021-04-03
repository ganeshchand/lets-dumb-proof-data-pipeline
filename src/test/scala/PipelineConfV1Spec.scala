import com.confix.PipelineConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.duration.Duration
import com.confix.PipelineConfV1
class PipelineConfSpec extends AnyFlatSpec with Matchers {
  implicit val env: String = "dev"
  def sharedAssertions(pipelineConf: PipelineConfV1) = {
    pipelineConf.rowsPerSecond mustBe 1
    pipelineConf.outputPath mustBe "/tmp/deltalake/sample/1/rate"
    pipelineConf.checkpointPath mustBe s"${pipelineConf.outputPath}/_checkpoint"
    pipelineConf.triggerInterval mustBe Duration("30 seconds")
  }

  it must "be able to load configurations from HOCON String" in {
    val pipelineConfHocon =
      """pipeline.conf {
        |  triggerInterval: 30s
        |  outputPath: /tmp/deltalake/sample/1/rate
        |  checkpointPath: ${pipeline.conf.outputPath}"/_checkpoint" // derived configuration: concatenation and substitution
        |  rowsPerSecond: 1
        |}""".stripMargin

    val pipelineConf = PipelineConfV1.loadFromString(pipelineConfHocon)
    sharedAssertions(pipelineConf)
  }

  it must "be able to load configurations from resources folder" in {
    val pipelineConf: PipelineConfV1 = PipelineConfV1.loadFromResource("ingest_stream_rate_sample.conf")
    sharedAssertions(pipelineConf)
  }

  it must "be able to load configurations from local file path " in {
    val filePath = getClass.getResource("ingest_stream_rate_sample.conf").getPath
    println(filePath)
    val pipelineConf: PipelineConfV1 = PipelineConfV1.loadFromFile(path = filePath)
    sharedAssertions(pipelineConf)
  }

}
