package example.exampleProducer01

import io.github.nomisRev.kafka.*
import java.util.Properties
import kotlinx.coroutines.runBlocking

import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
@JvmInline value class Key(val index: Int)
@JvmInline value class Message(val content: String)

fun main() = runBlocking {
  val settings: ProducerSettings<Key, Message> = ProducerSettings(
    Kafka.container.bootstrapServers,
    IntegerSerializer().imap { key: Key -> key.index },
    StringSerializer().imap { msg: Message -> msg.content },
    Acks.All
  )
  (1..10)
    .map { index -> ProducerRecord("example-topic", Key(index), Message("msg: $index")) }
    .asFlow()
    .produce(settings)
    .collect(::println)
}
