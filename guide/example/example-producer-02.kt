package example.exampleProducer02

import arrow.continuations.SuspendApp
import io.github.nomisRev.kafka.sendAwait
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

fun main() = SuspendApp {
  KafkaProducer(Properties(), StringSerializer(), StringSerializer()).use { producer ->
    producer.sendAwait(ProducerRecord("topic-name", "message #1"))
    producer.sendAwait(ProducerRecord("topic-name", "message #2"))
  }
}
