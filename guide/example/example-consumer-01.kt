package example.exampleConsumer01

import arrow.continuations.SuspendApp
import io.github.nomisRev.kafka.ConsumerSettings
import io.github.nomisRev.kafka.kafkaConsumer
import io.github.nomisRev.kafka.map
import io.github.nomisRev.kafka.subscribeTo
import java.util.UUID
import kotlinx.coroutines.flow.map
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
@JvmInline value class Key(val index: Int)
@JvmInline value class Message(val content: String)

fun main() = SuspendApp {
  val settings: ConsumerSettings<Key, Message> = ConsumerSettings(
    Kafka.container.bootstrapServers,
    IntegerDeserializer().map(::Key),
    StringDeserializer().map(::Message),
    groupId = UUID.randomUUID().toString()
  )
  kafkaConsumer(settings)
    .subscribeTo("example-topic")
    .map { record -> record.value() }
    .collect { msg: Message -> println(msg) }
}
