package example.examplePublisher01

import arrow.continuations.SuspendApp
import io.github.nomisRev.kafka.imap
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.publisher.PublisherSettings
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
@JvmInline value class Key(val index: Int)
@JvmInline value class Message(val content: String)

fun main() = SuspendApp {
  val settings = PublisherSettings(
    Kafka.container.bootstrapServers,
    IntegerSerializer().imap { key: Key -> key.index },
    StringSerializer().imap { msg: Message -> msg.content },
  )

  KafkaPublisher(settings).use { publisher ->
    // ... use the publisher
    val m: Map<MetricName, Metric> = publisher.metrics()
    println(m)

    publisher.publishScope {
      // send record without awaiting acknowledgement
      offer(ProducerRecord("example-topic", Key(1), Message("msg-1")))

      // send record and suspends until acknowledged
      publish(ProducerRecord("example-topic", Key(2), Message("msg-2")))
    }
  }
}
