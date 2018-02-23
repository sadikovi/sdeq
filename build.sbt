name := "sdeq"

organization := "com.github.sadikovi"

scalaVersion := "2.11.7"

spName := "sadikovi/sdeq"

val defaultSparkVersion = "2.2.1"

sparkVersion := sys.props.getOrElse("spark.testVersion", defaultSparkVersion)

val defaultHadoopVersion = "2.7.0"

val hadoopVersion = settingKey[String]("The version of Hadoop to test against.")

hadoopVersion := sys.props.getOrElse("hadoop.testVersion", defaultHadoopVersion)

spAppendScalaVersion := true

spIncludeMaven := false

spIgnoreProvided := true

sparkComponents := Seq("sql")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion.value % "test" exclude("javax.servlet", "servlet-api") force(),
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client")
)

// Project dependencies provided by Spark but not included in artefacts
libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.6.2.Final" % "provided",
  "com.google.guava" % "guava" % "14.0.1" % "provided"
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

// Check deprecation without manual restart
scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

// Display full-length stacktraces from ScalaTest
testOptions in Test += Tests.Argument("-oF")

parallelExecution in Test := false
