package example.exampleConsumer01

import io.github.nomisRev.kafka.*
import java.util.Properties
import kotlinx.coroutines.runBlocking

import java.util.UUID
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.collect
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
@JvmInline value class Key(val index: Int)
@JvmInline value class Message(val content: String)

fun main() = runBlocking {
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
