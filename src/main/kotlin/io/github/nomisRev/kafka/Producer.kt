@file:JvmName("Producer")

package io.github.nomisRev.kafka

import java.util.Properties
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serializer

/**
 * Produces all [ProducerRecord] in this [Flow] with the provided [ProducerSettings].
 * A new [KafkaProducer] is creating from the [ProducerSettings] and,
 * all [ProducerRecord] are published *in-order* in a synchronous way.
 *
 * <!--- INCLUDE
 * import kotlinx.coroutines.flow.asFlow
 * import kotlinx.coroutines.flow.collect
 * import org.apache.kafka.clients.producer.ProducerRecord
 * import org.apache.kafka.common.serialization.IntegerSerializer
 * import org.apache.kafka.common.serialization.StringSerializer
 * @JvmInline value class Key(val index: Int)
 * @JvmInline value class Message(val content: String)
 * -->
 * ```kotlin
 * fun main() = runBlocking {
 *   val settings: ProducerSettings<Key, Message> = ProducerSettings(
 *     Kafka.container.bootstrapServers,
 *     IntegerSerializer().imap { key: Key -> key.index },
 *     StringSerializer().imap { msg: Message -> msg.content },
 *     Acks.All
 *   )
 *   (1..10)
 *     .map { index -> ProducerRecord("example-topic", Key(index), Message("msg: $index")) }
 *     .asFlow()
 *     .produce(settings)
 *     .collect(::println)
 * }
 * ```
 * <!--- KNIT example-producer-01.kt -->
 */
@FlowPreview
public suspend fun <A, B> Flow<ProducerRecord<A, B>>.produce(
  settings: ProducerSettings<A, B>,
): Flow<RecordMetadata> =
  kafkaProducer(settings).flatMapConcat { producer ->
    this@produce.map { record -> producer.sendAwait(record) }
  }

/**
 * Sends a record to a Kafka topic in a suspending way.
 *
 * <!--- INCLUDE
 * import org.apache.kafka.clients.producer.KafkaProducer
 * import org.apache.kafka.clients.producer.ProducerRecord
 * import org.apache.kafka.common.serialization.StringSerializer
 * -->
 * ```kotlin
 * fun main() = runBlocking<Unit> {
 *   KafkaProducer(Properties(), StringSerializer(), StringSerializer()).use { producer ->
 *     producer.sendAwait(ProducerRecord("topic-name", "message #1"))
 *     producer.sendAwait(ProducerRecord("topic-name", "message #2"))
 *   }
 * }
 * ```
 * <!--- KNIT example-producer-02.kt -->
 */
public suspend fun <A, B> KafkaProducer<A, B>.sendAwait(
  record: ProducerRecord<A, B>,
): RecordMetadata =
  suspendCoroutine { cont ->
    // Those can be a SerializationException when it fails to serialize the message,
    // a BufferExhaustedException or TimeoutException if the buffer is full,
    // or an InterruptException if the sending thread was interrupted.
    send(record) { a, e ->
      // null if an error occurred, see: org.apache.kafka.clients.producer.Callback
      if (a != null) cont.resume(a) else cont.resumeWithException(e)
    }
  }

/**
 * KafkaKafkaProducer for [K] - [V] which takes
 */
@Suppress("FunctionName")
public fun <K, V> KafkaProducer(setting: ProducerSettings<K, V>): KafkaProducer<K, V> =
  KafkaProducer(setting.properties(), setting.keyDeserializer, setting.valueDeserializer)

/**
 * Will automatically close, and flush when finished streaming.
 * The [Flow] will close when the [KafkaProducer] is consumed from the [Flow].
 *
 * This means that the [KafkaProducer] will not be closed for a synchronous running stream, but
 * when running the [Flow] is offloaded in a separate Coroutine it's prone to be collected, closed
 * and flushed. In the example below we construct a producer stream that produces 100 indexed
 * messages.
 *
 * ```kotlin
 * fun <Key, Value> KafkaProducer<Key, Value>.produce(topicName: String, count: Int): Flow<Unit> =
 *   (0..count).asFlow().map { sendAwait(ProducerRecord(topicName, "message #it")) }
 *
 * val producerStream = kafkaProducer(Properties(), StringSerializer(), StringSerializer())
 *   .flatMapConcat { producer -> producer.produce("topic-name", 100) }
 * ```
 *
 * Here the `KafkaProducer` will only get collected (and closed/flushed) when all 100 messages
 * were produced.
 *
 * **DO NOT** If instead we'd do something like the following, where we offload in a buffer then
 * the `KafkaProducer` gets collected into the buffer and thus closed/flushed.
 *
 * ```kotlin
 * kafkaProducer(Properties(), StringSerializer(), StringSerializer()).buffer(10)
 * ```
 */
public fun <K, V> kafkaProducer(
  setting: ProducerSettings<K, V>,
): Flow<KafkaProducer<K, V>> = flow {
  KafkaProducer(setting).use { producer ->
    try {
      emit(producer)
    } finally {
      producer.flush()
    }
  }
}

public enum class Acks(public val value: String) {
  All("all"),
  MinusOne("-1"),
  Zero("0"),
  One("1")
}

/**
 * A type-safe constructor for [KafkaProducer] settings.
 * It forces you to specify the bootstrapServer, and serializers for [K] and [V].
 * These are the minimum requirements for constructing a valid [KafkaProducer].
 *
 * @see http://kafka.apache.org/documentation.html#producerconfigs
 */
public data class ProducerSettings<K, V>(
  val bootstrapServers: String,
  val keyDeserializer: Serializer<K>,
  val valueDeserializer: Serializer<V>,
  val acks: Acks = Acks.One,
  val other: Properties? = null,
) {
  public fun properties(): Properties =
    Properties().apply {
      put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keyDeserializer::class.qualifiedName)
      put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueDeserializer::class.qualifiedName)
      put(ProducerConfig.ACKS_CONFIG, acks.value)
      other?.let { putAll(other) }
    }
}
