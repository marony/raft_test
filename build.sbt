name := """raft-test"""

version := "1.0"

scalaVersion := "2.11.8"

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(
  "com.github.nscala-time" %% "nscala-time" % "2.16.0",
  "ch.qos.logback" % "logback-classic" % "1.1.8",
  "com.typesafe.akka" %% "akka-slf4j" % "2.4.16",
  "com.typesafe.akka" %% "akka-actor" % "2.4.16",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)
