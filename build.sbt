ThisBuild / version := "0.1.0-SNAPSHOT"

//ThisBuild / scalaVersion := "2.11.12"
ThisBuild / scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "sbtIO"
  )

//libraryDependencies += "org.scala-saddle" %% "saddle-core" % "1.3.4"

val sparkVersion = "3.2.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.30"