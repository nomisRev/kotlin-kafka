package io.github.nomisRev.kafka

import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ConsumerSettings
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
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
import java.util.UUID

@JvmInline
value class Key(val index: Int)

@JvmInline
value class Message(val content: String)

fun main(): Unit = runBlocking(Dispatchers.Default) {
  val topicName = "test-topic"
  val msgCount = 25
  val kafka = Kafka.container
  
  Admin(AdminSettings(kafka.bootstrapServers)).use { client ->
    client.createTopic(NewTopic(topicName, 1, 1))
  }
  
  coroutineScope { // Run produces and consumer in a single scope
    launch(Dispatchers.IO) { // Send 20 messages, and then close the producer
      val settings: ProducerSettings<Key, Message> = ProducerSettings(
        kafka.bootstrapServers,
        IntegerSerializer().imap { key: Key -> key.index },
        StringSerializer().imap { msg: Message -> msg.content },
        Acks.All
      )
      (1..msgCount)
        .asFlow()
        .map { index ->
          delay(index * 50L)
          ProducerRecord(topicName, Key(index), Message("msg: $index"))
        }
        .produce(settings)
        .collect(::println)
    }
    
    launch(Dispatchers.IO) { // Consume 20 messages as a stream, and then close the consumer
      val settings: ConsumerSettings<Key, Message> = ConsumerSettings(
        kafka.bootstrapServers,
        IntegerDeserializer().map(::Key),
        StringDeserializer().map(::Message),
        groupId = UUID.randomUUID().toString(),
        autoOffsetReset = AutoOffsetReset.Earliest
      )
      KafkaReceiver(settings)
        .receive(topicName)
        .take(msgCount)
        .collect {
          delay(75)
          println("${Thread.currentThread().name} => ${it.key()} -> ${it.value()}")
          it.offset.acknowledge()
        }
    }
  }
}

fun <A> Flow<A>.tap(also: suspend (A) -> Unit): Flow<A> =
  map { it.also { also(it) } }
