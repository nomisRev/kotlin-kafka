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

@Deprecated("""
  Use KafkaPublisher, and the Publisher DSL instead.
  This will be removed in 1.0.0.
""",
  ReplaceWith(
    "KafkaProducer(setting.properties(), setting.keyDeserializer, setting.valueDeserializer)",
    "org.apache.kafka.clients.producer.KafkaProducer"
  )
)
public fun <K, V> KafkaProducer(setting: ProducerSettings<K, V>): KafkaProducer<K, V> =
  KafkaProducer(setting.properties(), setting.keyDeserializer, setting.valueDeserializer)

@Deprecated("""
  Use KafkaPublisher, and the Publisher DSL instead.
  This will be removed in 1.0.0.
""",
  ReplaceWith(
    "KafkaProducer(setting.properties(), setting.keyDeserializer, setting.valueDeserializer).asFlow()",
    "org.apache.kafka.clients.producer.KafkaProducer"
  )
)
public fun <K, V> kafkaProducer(
  setting: ProducerSettings<K, V>,
): Flow<KafkaProducer<K, V>> = flow {
  KafkaProducer(setting).asFlow()
}

@Deprecated(
  "Use io.github.nomisRev.kafka.publisher.Acks instead",
  ReplaceWith("this", "io.github.nomisRev.kafka.publisher.Acks")
)
typealias Acks =
  io.github.nomisRev.kafka.publisher.Acks

@Deprecated("""
  This has moved package, and has been renamed to PublisherSettings.
  The constructor has the same names, and thus matches the signature,
  and can easily be replaced. By using the PublisherSettings instead.
  
  This will be removed in 1.0.0
""",
  ReplaceWith(
    "PublisherSettings(bootstrapServers, keyDeserializer, valueDeserializer, acks, other)",
    "io.github.nomisRev.kafka.publisher.PublisherSettings"
  )
)
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
