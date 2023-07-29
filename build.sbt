ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "sbtIO"
  )

libraryDependencies += "org.scala-saddle" %% "saddle-core" % "1.3.4"