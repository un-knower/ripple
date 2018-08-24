val commonSettings = List(
  organization := "ink.baixin",
  scalaVersion := "2.12.6",
  version      := "0.1.0-SNAPSHOT"
)

lazy val producer = (project in file(".")).
  settings(
    name := "ripple-producer",
    inThisBuild(commonSettings),
    fork in run := true,
    javaOptions in run += "-XX:+PrintGCDetails",
    javaOptions in run += "-Xmx2G",
    javaOptions in run += "-Xmn1G",
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "io.netty" % "netty-all" % "4.1.25.Final",
      "com.amazonaws" % "amazon-kinesis-producer" % "0.12.9",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.11.0",
      "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
      "ink.baixin" %% "ripple-core" % "0.1.0-SNAPSHOT",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    ),
  ).enablePlugins(PackPlugin)