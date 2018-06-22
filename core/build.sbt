val jodaVersion = "2.9.9"

lazy val core = (project in file("."))
  .settings(
    name := "ripple-core",
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "com.typesafe" % "config" % "1.3.1",
      "joda-time" % "joda-time" % jodaVersion,
      "com.google.guava" % "guava" % "25.0-jre",
      "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.330",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    ),
    PB.targets in Compile := Seq(
      scalapb.gen(grpc=false) -> (sourceManaged in Compile).value
    )
  )