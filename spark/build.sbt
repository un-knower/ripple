val sparkVersion = "2.3.0"
val jodaVersion = "2.9.9"

lazy val commonSettings = List(
  organization := "ink.baixin",
  scalaVersion := "2.11.12",
  version      := "0.1.0-SNAPSHOT",
)

lazy val spark = (project in file(".")).
  settings(
    name := "ripple-spark",
    inThisBuild(commonSettings),
    test in assembly := {},
    assemblyJarName in assembly := "dime-jobs.jar",
    assemblyMergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.startsWith("META-INF") => MergeStrategy.discard
      case "about.html" => MergeStrategy.rename
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    assemblyShadeRules in assembly := Seq(
      // protobuf-java has a conflict with spark-stream-kinesis-asl, use this shade to resolve the problem
      ShadeRule.rename("com.google.protobuf.**" -> "protobufv2_6_1.@1")
        .inLibrary(
          "com.google.protobuf" % "protobuf-java" % "2.6.1",
          "com.amazonaws" % "aws-java-sdk-core" % "1.11.76",
          "com.amazonaws" % "aws-java-sdk-s3" % "1.11.76",
          "com.amazonaws" % "aws-java-sdk-sts" % "1.11.76",
          "com.amazonaws" % "aws-java-sdk-kms" % "1.11.76",
          "com.amazonaws" % "aws-java-sdk-kinesis" % "1.11.76",
          "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.76",
          "com.amazonaws" % "aws-java-sdk-cloudwatch" % "1.11.76",
          "com.amazonaws" % "amazon-kinesis-client" % "1.7.3",
          "org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVersion,
        ).inProject
    ),
    crossScalaVersions := Seq("2.11.12"),
    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % jodaVersion,
      "com.github.scopt" %% "scopt" % "3.7.0",
      "com.typesafe" % "config" % "1.3.1",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "ink.baixin" %% "ripple-core" % "0.1.0-SNAPSHOT" exclude("com.google.protobuf", "protobuf-java"),

      "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
      "org.apache.spark" % "spark-yarn_2.11" % sparkVersion % "provided",
      "org.apache.spark" % "spark-streaming_2.11" % sparkVersion % "provided",
      "org.apache.spark" % "spark-streaming-kinesis-asl_2.11" % sparkVersion % "provided",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    )
  )