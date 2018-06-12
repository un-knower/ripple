lazy val query = (project in file(".")).
  settings(
    name := "ripple-query",
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",

      "org.apache.calcite" % "calcite-core" % "1.16.0",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    )
  )