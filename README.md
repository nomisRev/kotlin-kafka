Module kotlin-kafka

<!--- TEST_NAME ReadmeTest -->
<!--- TOC -->

* [Rationale](#rationale)
* [Goals](#goals)
* [Example](#example)

<!--- END -->

This project is still under development, andd started as a playground where I was playing around with Kafka in Kotlin and the Kafka SDK whilst reading the Kafka book Definite Guide from Confluent.
https://www.confluent.io/resources/kafka-the-definitive-guide-v2/

## Rationale

At the time of starting this repository I didn't find any bindings between Kafka SDK and Kotlin suspension.
These operators should be implemented low-level, so they can guarantee correct cancellation support, and high optimised runtimes.

## Goals
 - Lean Core library built on top of Kotlin Std & KotlinX Coroutines (possible extensions with Arrow in additional module)
 - Extensions to easily operate over the Kafka SDK with KotlinX Coroutines and `suspend`.
 - Flow based operators, so you can easily compose KotlinX Flow based Kafka programs
 - example for testing Kafka with Test Containers in Kotlin.

## Example

<!--- INCLUDE
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
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

fun kafkaContainer(
  fullImageName: String = "confluentinc/cp-kafka:6.2.1"
): Resource<KafkaContainer> = resource {
  val kafka =
    Resource.fromAutoCloseable { KafkaContainer(DockerImageName.parse(fullImageName)) }.bind()
  // start is an interruptible blocking Java method
  runInterruptible(IO) { kafka.start() }
  kafka
}
-->
```kotlin
@JvmInline
value class Key(val index: Int)

@JvmInline
value class Message(val content: String)

fun main(): Unit =
  runBlocking(Default) {
    kafkaContainer().use { kafka ->
      val topicName = "test-topic"
      val msgCount = 20

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
  }
```

> You can get the full code [here](guide/example/example-readme-01.kt).

```text
test-topic-0@0
test-topic-0@1
test-topic-0@2
test-topic-0@3
test-topic-0@4
test-topic-0@5
test-topic-0@6
test-topic-0@7
test-topic-0@8
test-topic-0@9
test-topic-0@10
test-topic-0@11
test-topic-0@12
test-topic-0@13
test-topic-0@14
test-topic-0@15
test-topic-0@16
test-topic-0@17
test-topic-0@18
test-topic-0@19
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
Key(index=11) -> Message(content=msg: 11)
Key(index=12) -> Message(content=msg: 12)
Key(index=13) -> Message(content=msg: 13)
Key(index=14) -> Message(content=msg: 14)
Key(index=15) -> Message(content=msg: 15)
Key(index=16) -> Message(content=msg: 16)
Key(index=17) -> Message(content=msg: 17)
Key(index=18) -> Message(content=msg: 18)
Key(index=19) -> Message(content=msg: 19)
Key(index=20) -> Message(content=msg: 20)
```
<!--- TEST -->
