import com.github.nomisRev.kafka.NoOpSerializer
import com.github.nomisRev.kafka.createTopic
import com.github.nomisRev.kafka.describeTopic
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

suspend fun main(): Unit = runBlocking(Dispatchers.Default) {
  launchKafka().use { (default, client) ->
    val topicName = "test-topic"
    val msgCount = 20

    client.createTopic(NewTopic(topicName, 1, 1))
    println("Topic${client.describeTopic(topicName)}")

    coroutineScope { // Run produces and consumer in a single scope
      launch { // Send 20 messages, and then close the producer
        (1..msgCount)
          .map { ProducerRecord<Nothing, String>(topicName, "msg: $it") }
          .asFlow()
          .produce(default, NoOpSerializer, StringSerializer())
          .collect(::println)
      }

      launch { // Consume 20 messages as a stream, and then close the consumer
        kafkaConsumer(default, StringDeserializer(), StringDeserializer())
          .subscribeTo(topicName)
          .take(msgCount)
          .map { it.value() }
          .collect(::println)
      }
    }
  }
}
