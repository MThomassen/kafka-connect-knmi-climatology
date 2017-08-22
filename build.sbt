val kafkaConnectVersion = "0.11.0.0"

val `kafka-connect-knmi-climatology` = project
  .in(file("."))
  .settings(
    name := "kafka-connect-knmi-climatology",
    organization := "MThomassen",
    scalaVersion := "2.11.11",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "connect-api" % kafkaConnectVersion,
      "com.typesafe.akka" %% "akka-http" % "10.0.9",
      "com.typesafe.akka" %% "akka-stream" % "2.5.4",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "com.typesafe.akka" %% "akka-testkit" % "2.5.4" % Test,
      "org.scalatest" %% "scalatest" % "3.0.4" % Test),
    parallelExecution in Test := false,
    fork in run := true
  )