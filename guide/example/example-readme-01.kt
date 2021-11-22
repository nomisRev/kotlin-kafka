package example.exampleReadme01

import com.github.nomisRev.kafka.*
import java.util.Properties
import kotlinx.coroutines.runBlocking

import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.computations.resource
import arrow.fx.coroutines.fromAutoCloseable
import com.github.nomisRev.kafka.Acks
import com.github.nomisRev.kafka.AdminSettings
import com.github.nomisRev.kafka.AutoOffsetReset
import com.github.nomisRev.kafka.ConsumerSettings
import com.github.nomisRev.kafka.ProducerSettings
import com.github.nomisRev.kafka.Admin
import com.github.nomisRev.kafka.createTopic
import com.github.nomisRev.kafka.kafkaConsumer
import com.github.nomisRev.kafka.map
import com.github.nomisRev.kafka.imap
import com.github.nomisRev.kafka.produce
import com.github.nomisRev.kafka.subscribeTo
import java.util.UUID
import kotlinx.coroutines.Dispatchers.Default
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

@JvmInline
value class Key(val index: Int)

@JvmInline
value class Message(val content: String)

fun main(): Unit =
  runBlocking(Default) {
    val topicName = "test-topic"
    val msgCount = 10
    val kafka = Kafka.container

    Admin(AdminSettings(kafka.bootstrapServers)).use { client ->
      client.createTopic(NewTopic(topicName, 1, 1))
    }

    coroutineScope { // Run produces and consumer in a single scope
      launch { // Send 20 messages, and then close the producer
        val settings: ProducerSettings<Key, Message> =
          ProducerSettings(
            kafka.bootstrapServers,
            IntegerSerializer().imap { key: Key -> key.index },
            StringSerializer().imap { msg: Message -> msg.content },
            Acks.All
          )
        (1..msgCount)
          .map { index -> ProducerRecord(topicName, Key(index), Message("msg: $index")) }
          .asFlow()
          .produce(settings)
          .collect(::println)
      }

      launch { // Consume 20 messages as a stream, and then close the consumer
        val settings: ConsumerSettings<Key, Message> =
          ConsumerSettings(
            kafka.bootstrapServers,
            IntegerDeserializer().map(::Key),
            StringDeserializer().map(::Message),
            groupId = UUID.randomUUID().toString(),
            autoOffsetReset = AutoOffsetReset.Earliest
          )
        kafkaConsumer(settings)
          .subscribeTo(topicName)
          .take(msgCount)
          .map { "${it.key()} -> ${it.value()}" }
          .collect(::println)
      }
    }
  }
