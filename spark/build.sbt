val sparkVersion = "2.2.1"
val jodaVersion = "2.9.9"

lazy val commonSettings = List(
  organization := "ink.baixin",
  scalaVersion := "2.12.6",
  version      := "0.1.0-SNAPSHOT"
)

lazy val spark = (project in file(".")).
  settings(
    name := "ripple-spark",
    inThisBuild(commonSettings),
    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % jodaVersion,
      "com.github.scopt" %% "scopt" % "3.7.0",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",

      "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
      "org.apache.spark" % "spark-yarn_2.11" % sparkVersion % "provided",
      "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",

      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    )
  )