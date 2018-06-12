val commonSettings = List(
  organization := "ink.baixin",
  scalaVersion := "2.12.6",
  version      := "0.1.0-SNAPSHOT"
)

lazy val core = (project in file("core")).settings(inThisBuild(commonSettings))
lazy val query = (project in file("query")).settings(inThisBuild(commonSettings))
lazy val spark = (project in file("spark")).settings(inThisBuild(commonSettings)).dependsOn(core)

lazy val ripple = (project in file("."))
    .settings(
      name := "ripple",
      commonSettings
    ).aggregate(core, query)