name := "sdeq"

organization := "com.github.sadikovi"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.1"
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last.endsWith( ".html") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last == "UnusedStubClass.class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// Check deprecation without manual restart
scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

// Display full-length stacktraces from ScalaTest
testOptions in Test += Tests.Argument("-oF")

parallelExecution in Test := false
