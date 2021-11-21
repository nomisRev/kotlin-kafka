import com.github.nomisRev.kafka.Acks
import com.github.nomisRev.kafka.AdminSettings
import com.github.nomisRev.kafka.AutoOffsetReset
import com.github.nomisRev.kafka.ConsumerSettings
import com.github.nomisRev.kafka.NothingSerializer
import com.github.nomisRev.kafka.ProducerSettings
import com.github.nomisRev.kafka.adminClient
import com.github.nomisRev.kafka.createTopic
import com.github.nomisRev.kafka.describeTopic
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

suspend fun main(): Unit =
  runBlocking(Dispatchers.Default) {
    kafkaContainer().use { kafka ->
      val topicName = "test-topic"
      val msgCount = 20

      adminClient(AdminSettings(kafka.bootstrapServers)).use { client ->
        client.createTopic(NewTopic(topicName, 1, 1))
        println("Topic${client.describeTopic(topicName)}")
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
