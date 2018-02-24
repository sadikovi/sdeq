name := "sdeq"

organization := "com.github.sadikovi"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided"
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
