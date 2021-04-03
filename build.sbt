
name := "dumbProofPipeline"

version := "0.0.1"

scalaVersion := "2.12.13"

lazy val dumbProofPipeline = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.databricks",
    libraryDependencies ++= commonDependencies
  )

// dependencies

val sparkVersion = "3.0.1"
val deltaVersion = "0.7.0"

lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "io.delta" %% "delta-core" % deltaVersion % Provided,
  "org.scalatest" %% "scalatest" % "3.2.3" % Test,
  "com.github.pureconfig" %% "pureconfig" % "0.14.0"
)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + version.value,
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case ".conf"            => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

)

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {f => f.data.getName.startsWith("scala-")}
}

assemblyShadeRules in assembly ++= Seq(
  ShadeRule.rename("com.typesafe.config.**" -> "dumbproof.@1")
    .inLibrary("com.typesafe" % "config" % "1.4.0")
    .inProject
)

