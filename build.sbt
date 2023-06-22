ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "youtube_analyzer"
  )

val sparkVersion = "3.4.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % "3.4.0",
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.0",
  "org.postgresql" % "postgresql" % "42.5.4",
  "com.google.apis" % "google-api-services-youtube" % "v3-rev222-1.18.0-rc",

  "org.apache.kafka" % "kafka-clients" % sparkVersion,
  "org.apache.kafka" % "kafka-streams" % sparkVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % sparkVersion,

  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-generic" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5"
)
libraryDependencies += "com.google.code.gson" % "gson" % "2.10.1"
libraryDependencies += "com.google.http-client" % "google-http-client-gson" % "1.43.1"
libraryDependencies ++= sparkDependencies


