@file:JvmName("Producer")

package io.github.nomisRev.kafka

import io.github.nomisRev.kafka.publisher.PublisherSettings
import io.github.nomisRev.kafka.publisher.produceOrThrow
import java.util.Properties
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serializer


@Deprecated(
  """
  Use KafkaPublisher, and produceOrThrow instead.
  This will send records to Kafka without awaiting the acknowledgement,
  and resulting in maximum throughput.
  
  If any error occurs, it will throw an exception and any subsequent records will not be sent.
  You can use `Flow.catch` to handle any errors, to prevent the flow from being cancelled.
  Or, use [Flow.produce] to send records, and receive `Result<Throwable>` instead of using `Flow.catch`.
  
  This will be removed in 1.0.0.
""",
  ReplaceWith(
    "produceOrThrow(settings.toPublisherSettings())",
    "io.github.nomisRev.kafka.publisher.produceOrThrow"
  )
)
public fun <A, B> Flow<ProducerRecord<A, B>>.produce(
  settings: ProducerSettings<A, B>,
): Flow<RecordMetadata> =
  produceOrThrow(settings.toPublisherSettings())

/**
 * Sends a record to a Kafka topic in a suspending way.
 *
 * <!--- INCLUDE
 * import arrow.continuations.SuspendApp
 * import io.github.nomisRev.kafka.sendAwait
 * import org.apache.kafka.clients.producer.KafkaProducer
 * import org.apache.kafka.clients.producer.ProducerRecord
 * import org.apache.kafka.common.serialization.StringSerializer
 * import java.util.Properties
 * -->
 * ```kotlin
 * fun main() = SuspendApp {
 *   KafkaProducer(Properties(), StringSerializer(), StringSerializer()).use { producer ->
 *     producer.sendAwait(ProducerRecord("topic-name", "message #1"))
 *     producer.sendAwait(ProducerRecord("topic-name", "message #2"))
 *   }
 * }
 * ```
 * <!--- KNIT example-producer-01.kt -->
 */
@Deprecated(
  """
    Use KafkaPublisher, and the Publisher DSL instead.
    sendAwait is a slow, since it awaits an acknowledgement from Kafka.
    Resulting in a lower throughput. This will be removed in 1.0.0.
  """
)
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

@Deprecated(
  "Use io.github.nomisRev.kafka.publisher.Acks instead",
  ReplaceWith("this", "io.github.nomisRev.kafka.publisher.Acks")
)
typealias Acks =
  io.github.nomisRev.kafka.publisher.Acks

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

  fun toPublisherSettings(): PublisherSettings<K, V> =
    PublisherSettings(
      bootstrapServers,
      keyDeserializer,
      valueDeserializer,
      acks,
      properties = other ?: Properties()
    )
}

public operator fun <K, V> ProducerRecord<K, V>.component1(): K = key()
public operator fun <K, V> ProducerRecord<K, V>.component2(): V = value()
