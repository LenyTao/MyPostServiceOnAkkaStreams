import sbt._

object Dependencies {
  val scalaCompat = "2.12.10"

  private object Version {
    val akka = "2.6.14"
  }

  object Akka {
    val AkkaTyped          = "com.typesafe.akka" %% "akka-actor-typed"  % Version.akka
    val Stream             = "com.typesafe.akka" %% "akka-stream"       % Version.akka
    val ConnectorAkkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.7"
  }

  object Kafka {
    val Kafka = "org.apache.kafka" % "kafka-clients" % "2.3.1"
  }

  object Logging {
    val Slf4j        = "org.slf4j"                  % "slf4j-simple"    % "1.6.2"
    val ScalaLogging = "com.typesafe.scala-logging" %% "scala-logging"  % "3.9.3"
    val Logback      = "ch.qos.logback"             % "logback-classic" % "1.2.3"
  }

  object Testing {
    val ScalaTest = "org.scalatest" %% "scalatest" % "3.2.7" % "test"
  }

  object Config {
    val Config = "com.typesafe" % "config" % "1.4.1"
  }

  object JsonConverter {
    val Converter = "io.spray" %% "spray-json" % "1.3.6"
  }

  object JavaMail {
    val JavaMail = "com.sun.mail" % "javax.mail" % "1.6.0"
  }
}
