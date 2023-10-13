Module kotlin-kafka

[![Maven Central](https://img.shields.io/maven-central/v/io.github.nomisrev/kotlin-kafka?color=4caf50&label=latest%20release)](https://maven-badges.herokuapp.com/maven-central/io.github.nomisrev/kotlin-kafka)

<!--- TOC -->

* [Rationale](#rationale)
* [Goals](#goals)
* [Adding Dependency](#adding-dependency)
* [Example](#example)

<!--- END -->

## Rationale

At the time of starting this repository I didn't find any bindings between Kafka SDK and Kotlin suspension, or KotlinX Coroutines Flow.
These operators should be implemented low-level, so they can guarantee correct cancellation support, and high optimised runtimes.

Some important aspects of Kafka are tricky to implement with the "low-level" Kafka API,
especially properly streaming records from Kafka and correctly committing them.
Additional complexity is involved in this process, more details [here](https://tuleism.github.io/blog/2021/parallel-backpressured-kafka-consumer/).

To solve these problems a couple of projects in the JVM already exist:
 - [Alpakka Kafka](https://github.com/akka/alpakka-kafka)
 - [reactor-kafka](https://github.com/reactor/reactor-kafka)

There was no implementation for KotlinX Coroutines Flow,
you can however quite easily use reactor-kafka with [KotlinX Coroutines Reactor bindings](https://github.com/Kotlin/kotlinx.coroutines/blob/master/reactive/kotlinx-coroutines-reactor/README.md).

This project implements the same strategies as [reactor-kafka] directly on top of KotlinX Coroutines to benefit from **all** their benefits,
and to open the door to potentially becoming a Kotlin MPP library in the future.

## Goals

- Lean Core library built on top of Kotlin Std & KotlinX Coroutines
- Extensions to easily operate over the Kafka SDK with KotlinX Coroutines and `suspend`.
- Flow based operators, so you can easily compose KotlinX Flow based Kafka programs
- Strong guarantees about committing record offsets, and performance optimisations in regard to re-balancing/partitioning.
- example for testing Kafka with Test Containers in Kotlin.

## Adding Dependency

Simply add the following dependency as `implementation` in the `build.gradle` dependencies` block.

```groovy
dependencies {
  implementation("io.github.nomisrev:kotlin-kafka:0.3.0")
}
```

## Example

<!--- INCLUDE
import arrow.continuations.SuspendApp
import io.github.nomisRev.kafka.Admin
import io.github.nomisRev.kafka.AdminSettings
import io.github.nomisRev.kafka.createTopic
import io.github.nomisRev.kafka.imap
import io.github.nomisRev.kafka.map
import io.github.nomisRev.kafka.publisher.Acks
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.publisher.PublisherSettings
import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import kotlinx.coroutines.Dispatchers
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
-->

```kotlin
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
    val settings: PublisherSettings<Key, Message> = PublisherSettings(
      kafka.bootstrapServers,
      IntegerSerializer().imap { key: Key -> key.index },
      StringSerializer().imap { msg: Message -> msg.content },
      Acks.All
    )
    KafkaPublisher(settings).use { publisher ->
      publisher.publishScope {
        (1..msgCount).forEach { index ->
          offer(ProducerRecord(topicName, Key(index), Message("msg: $index")))
        }
      }
    }
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
```

> You can get the full code [here](guide/example/example-readme-01.kt).

```text
Key(index=1) -> Message(content=msg: 1)
Key(index=2) -> Message(content=msg: 2)
Key(index=3) -> Message(content=msg: 3)
Key(index=4) -> Message(content=msg: 4)
Key(index=5) -> Message(content=msg: 5)
Key(index=6) -> Message(content=msg: 6)
Key(index=7) -> Message(content=msg: 7)
Key(index=8) -> Message(content=msg: 8)
Key(index=9) -> Message(content=msg: 9)
Key(index=10) -> Message(content=msg: 10)
```
