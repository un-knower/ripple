val jodaVersion = "2.9.9"
lazy val commonSettings = List(
  organization := "ink.baixin",
  scalaVersion := "2.12.6",
  version      := "0.1.0-SNAPSHOT"
)

lazy val core = (project in file("."))
  .settings(
    name := "ripple-core",
    inThisBuild(commonSettings),
    crossScalaVersions := Seq("2.11.12"),
    test in assembly := {},
    assemblyShadeRules in assembly := Seq(
      // protobuf-java has a conflict with spark-stream-kinesis-asl, use this shade to resolve the problem
      ShadeRule.rename("com.google.protobuf.**" -> "com.google.protobufv3_5.@1")
        .inLibrary(
          "com.google.protobuf" % "protobuf-java" % "3.5.0",
          "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.330"
        ).inProject
    ),
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "com.typesafe" % "config" % "1.3.1",
      "joda-time" % "joda-time" % jodaVersion,
      "com.google.guava" % "guava" % "25.0-jre",
      "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.330",

      // logging dependencies are only for testing under sub project
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.0" % Test,
      "org.apache.logging.log4j" % "log4j-core" % "2.11.0" % Test,
      "org.apache.logging.log4j" % "log4j-api" % "2.11.0" % Test,
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    ),
    PB.targets in Compile := Seq(
      scalapb.gen(grpc=false) -> (sourceManaged in Compile).value
    )
  )