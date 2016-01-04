name := "Meglytics-BI CORE"

version := "0.1"

scalaVersion := "2.11.7"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies ++= Seq (
  "spark.jobserver" %% "job-server-api" % "0.6.0",
  "org.apache.spark" %% "spark-core" % "1.5.1",
  "com.typesafe.akka" %% "akka-actor" % "2.3.9"


)
