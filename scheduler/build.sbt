val commonSettings = List(
  organization := "ink.baixin",
  scalaVersion := "2.12.6",
  version := "0.1.0-SNAPSHOT"
)

lazy val scheduler = (project in file(".")).
  settings(
    name := "ripple-scheduler",
    inThisBuild(commonSettings),
    libraryDependencies ++= Seq(
      // Logging
      // "org.slf4j" % "slf4j-log4j12" % "1.7.25",
      "org.apache.logging.log4j" % "log4j-core" % "2.11.0",
      "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",

      // Config
      "com.typesafe" % "config" % "1.3.1",

      // AWS
      "com.github.seratch" %% "awscala" % "0.6.1",

      // Akka
      "com.typesafe.akka" %% "akka-http" % "10.0.10",
      "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.10",

      // Spray-JSON
      "io.spray" %% "spray-json" % "1.3.3",

      // Joda time handling
      "joda-time" % "joda-time" % "2.9.9",

      // hadoop client related
      "org.apache.hadoop" % "hadoop-common" % "2.9.0",
      "org.apache.hadoop" % "hadoop-hdfs" % "2.9.0",
      "org.apache.hadoop" % "hadoop-yarn-client" % "2.9.0",

      // Hbase client related
      "org.apache.hbase" % "hbase-common" % "1.3.1",
      "org.apache.hbase" % "hbase-client" % "1.3.1",
      "org.apache.hbase" % "hbase-server" % "1.3.1",
      "org.apache.hive" % "hive-jdbc" % "2.3.2",

      // AWS extra
      "com.amazonaws" % "aws-java-sdk-elasticloadbalancingv2" % "1.11.184",

      // SlackBot Related
      "com.github.scopt" %% "scopt" % "3.7.0",
      "com.github.gilbertw1" %% "slack-scala-client" % "0.2.2"
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.apache.logging.log4j", "log4j-slf4j-impl"),
      ExclusionRule("log4j", "log4j")
    ),
    packGenerateWindowsBatFile := false
  ).enablePlugins(PackPlugin)