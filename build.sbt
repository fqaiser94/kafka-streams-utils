name := "kafka-streams-utils"

version := "0.1"

scalaVersion := "2.12.10"

val kafkaVersion = "2.6.0"
val confluentVersion = "6.0.0"
libraryDependencies ++= Seq(
  "io.github.azhur" %% "kafka-serde-avro4s" % "0.5.0",
  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion,
  "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion,
  "org.scalatest" %% "scalatest" % "3.2.2",
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion % Test
)

resolvers ++= Seq(
  "Confluent" at "https://packages.confluent.io/maven/",
  "jitpack" at "https://jitpack.io"
)

parallelExecution in Test := false

scalafmtOnCompile := true

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.concat
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}