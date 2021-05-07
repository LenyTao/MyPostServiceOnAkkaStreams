package com.example.notificator.producer

import com.example.notificator.Event
import com.example.notificator.notificationservice.JsonConverter
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.serialization.{ IntegerSerializer, StringSerializer }

import java.util.Properties

object Producer extends App with JsonConverter {
  private val topicName = "notificationMessageStore"

  private val producerProperties = new Properties()
  producerProperties.setProperty(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
    "localhost:9092"
  )
  producerProperties.setProperty(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    classOf[IntegerSerializer].getName
  )
  producerProperties.setProperty(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    classOf[StringSerializer].getName
  )

  private val producer = new KafkaProducer[Int, String](producerProperties)

  def sendToKafka(event: Event): Unit = {
    producer
      .send(new ProducerRecord[Int, String](topicName, 0, converterEvents.write(event).toString()))
  }

  TestEventCollection.getTestCollection.foreach(event => sendToKafka(event))

  producer.flush()
}
