# Kotlin Kafka

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
import com.github.nomisRev.kafka.NothingSerializer
import com.github.nomisRev.kafka.ProducerSettings
import com.github.nomisRev.kafka.adminClient
import com.github.nomisRev.kafka.createTopic
import com.github.nomisRev.kafka.kafkaConsumer
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
fun main(): Unit =
  runBlocking(Default) {
    kafkaContainer().use { kafka ->
      val topicName = "test-topic"
      val msgCount = 20

      adminClient(AdminSettings(kafka.bootstrapServers)).use { client ->
        client.createTopic(NewTopic(topicName, 1, 1))
      }

      coroutineScope { // Run produces and consumer in a single scope
        launch { // Send 20 messages, and then close the producer
          val settings =
            ProducerSettings(
              kafka.bootstrapServers,
              NothingSerializer,
              StringSerializer(),
              Acks.All
            )
          (1..msgCount)
            .map { ProducerRecord<Nothing, String>(topicName, "msg: $it") }
            .asFlow()
            .produce(settings)
            .collect(::println)
        }

        launch { // Consume 20 messages as a stream, and then close the consumer
          val settings =
            ConsumerSettings(
              kafka.bootstrapServers,
              StringDeserializer(),
              StringDeserializer(),
              groupId = UUID.randomUUID().toString(),
              autoOffsetReset = AutoOffsetReset.Earliest
            )
          kafkaConsumer(settings)
            .subscribeTo(topicName)
            .take(msgCount)
            .map { it.value() }
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
msg: 1
msg: 2
msg: 3
msg: 4
msg: 5
msg: 6
msg: 7
msg: 8
msg: 9
msg: 10
msg: 11
msg: 12
msg: 13
msg: 14
msg: 15
msg: 16
msg: 17
msg: 18
msg: 19
msg: 20
```
<!--- TEST -->
