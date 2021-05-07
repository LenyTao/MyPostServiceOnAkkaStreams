import Dependencies._

name := "MyPostServiceOnAkkaStreams"

version := "0.1"

scalaVersion := s"$scalaCompat"

libraryDependencies ++= Seq(
  Logging.Slf4j,
  Logging.ScalaLogging,
  Logging.Logback,
  Config.Config,
  JsonConverter.Converter,
  Akka.Stream,
  Akka.AkkaTyped,
  Akka.ConnectorAkkaKafka,
  Kafka.Kafka,
  JavaMail.JavaMail,
  Testing.ScalaTest
)
