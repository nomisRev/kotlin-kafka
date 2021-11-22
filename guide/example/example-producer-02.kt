package example.exampleProducer02

import com.github.nomisRev.kafka.*
import java.util.Properties
import kotlinx.coroutines.runBlocking

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

fun main() = runBlocking<Unit> {
  KafkaProducer(Properties(), StringSerializer(), StringSerializer()).use { producer ->
    producer.sendAwait(ProducerRecord("topic-name", "message #1"))
    producer.sendAwait(ProducerRecord("topic-name", "message #2"))
  }
}
