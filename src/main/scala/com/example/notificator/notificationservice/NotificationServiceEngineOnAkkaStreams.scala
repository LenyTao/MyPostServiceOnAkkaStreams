package com.example.notificator.notificationservice

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ ConsumerMessage, ConsumerSettings, Subscriptions }
import akka.stream.{ FlowShape, Graph }
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.{ Flow, GraphDSL, Source }
import com.example.notificator.Event
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ IntegerDeserializer, StringDeserializer }
import org.slf4j.LoggerFactory
import spray.json._

import java.time.Duration
import scala.concurrent.{ ExecutionContextExecutor, Future }

object NotificationServiceEngineOnAkkaStreams extends App with JsonConverter {
  private implicit val actorSystem: ActorSystem     = ActorSystem("NotificationSystem")
  private implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher

  private case class ClientAddressAndEvent(eMailAddress: Option[String], event: Event)

  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val collectionEmailAddressAndIDClients = ConfigReader.readClientList()

  private def getClientPost(event: Event): Option[String] = {
    val clientID = event.externalClientId.split("-")(0)
    collectionEmailAddressAndIDClients.get(clientID)
  }

  private val kafkaTopic = "notificationMessageStore"

  private val kafkaConsumerSettings: ConsumerSettings[Integer, String] = ConsumerSettings
    .create(actorSystem, new IntegerDeserializer(), new StringDeserializer())
    .withBootstrapServers(ConfigReader.readConnectionParameters().bootstrapServers)
    .withGroupId(ConfigReader.readConnectionParameters().groupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConfigReader.readConnectionParameters().property)
    .withStopTimeout(Duration.ofSeconds(ConfigReader.readConnectionParameters().timeout))

  private val kafkaSource: Source[ConsumerMessage.CommittableMessage[Integer, String], Consumer.Control] =
    Consumer.committableSource(kafkaConsumerSettings, Subscriptions.topics(kafkaTopic))

  private val converterFlow
    : Graph[FlowShape[ConsumerMessage.CommittableMessage[Integer, String], ClientAddressAndEvent], NotUsed] =
    GraphDSL.create[FlowShape[ConsumerMessage.CommittableMessage[Integer, String], ClientAddressAndEvent]]() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._

        val converterKafkaToClientAddressAndEvent = Flow[ConsumerMessage.CommittableMessage[Integer, String]]
          .map(event => converterEvents.read(event.record.value().parseJson))
          .map(event => ClientAddressAndEvent(getClientPost(event), event))

        val inputFlow  = builder.add(Flow[ConsumerMessage.CommittableMessage[Integer, String]])
        val outputFlow = builder.add(Flow[ClientAddressAndEvent])

        inputFlow.out ~> converterKafkaToClientAddressAndEvent ~> outputFlow.in

        FlowShape(inputFlow.in, outputFlow.out)
    }

  private val sinkSenderOfLetters: Sink[ClientAddressAndEvent, Future[Done]] =
    Sink.foreach {
      case client if client.eMailAddress.isDefined => SenderOfLetters.sendMail(client.eMailAddress.get, client.event)
      case client =>
        logger.error(
          s"Error: The email address for a customer with ID ${client.event.externalClientId} does not exist in Database"
        )
    }

  kafkaSource.via(converterFlow).runWith(sinkSenderOfLetters)
}
