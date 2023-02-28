package example.exampleReadme01

import io.github.nomisRev.kafka.*
import java.util.Properties
import kotlinx.coroutines.runBlocking

import arrow.continuations.SuspendApp
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.UUID

@JvmInline
value class Key(val index: Int)

@JvmInline
value class Message(val content: String)

fun main(): Unit = SuspendApp {
  val topicName = "test-topic"
  val msgCount = 10
  val kafka = Kafka.container

  Admin(AdminSettings(kafka.bootstrapServers)).use { client ->
    client.createTopic(NewTopic(topicName, 1, 1))
  }

  launch(Dispatchers.IO) { // Send 20 messages, and then close the producer
    val settings: ProducerSettings<Key, Message> = ProducerSettings(
      kafka.bootstrapServers,
      IntegerSerializer().imap { key: Key -> key.index },
      StringSerializer().imap { msg: Message -> msg.content },
      Acks.All
    )
    (1..msgCount)
      .asFlow()
      .map { index -> ProducerRecord(topicName, Key(index), Message("msg: $index")) }
      .produce(settings)
      .collect(::println)
  }

  launch(Dispatchers.IO) { // Consume 20 messages as a stream, and then close the consumer
    val settings: ReceiverSettings<Key, Message> = ReceiverSettings(
      kafka.bootstrapServers,
      IntegerDeserializer().map(::Key),
      StringDeserializer().map(::Message),
      groupId = UUID.randomUUID().toString(),
      autoOffsetReset = AutoOffsetReset.Earliest
    )
    KafkaReceiver(settings)
      .receive(topicName)
      .take(msgCount)
      .map { "${it.key()} -> ${it.value()}" }
      .collect(::println)
  }
}
