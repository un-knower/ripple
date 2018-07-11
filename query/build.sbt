lazy val query = (project in file(".")).
  settings(
    name := "ripple-query",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.1",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",

      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.11.0",
      "org.apache.logging.log4j" % "log4j-api" % "2.11.0",

      "sqlline" % "sqlline" % "1.3.0",
      "com.github.scopt" %% "scopt" % "3.7.0",
      "com.typesafe.akka" %% "akka-actor" % "2.5.12",
      "com.typesafe.akka" %% "akka-stream" % "2.5.12",
      "com.typesafe.akka" %% "akka-http" % "10.1.1",
      "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.1",
      "org.apache.calcite" % "calcite-core" % "1.16.0",

      "ink.baixin" %% "ripple-core" % "0.1.0-SNAPSHOT",

      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    )
  ).enablePlugins(PackPlugin)