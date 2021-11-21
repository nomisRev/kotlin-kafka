package com.github.nomisRev.kafka

import java.util.Properties
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.serialization.Serializer

public enum class Acks(public val value: String) {
  All("all"),
  MinusOne("-1"),
  Zero("0"),
  One("1")
}

public data class ProducerSettings<K, V>(
  // BOOTSTRAP_SERVERS_CONFIG
  val bootstrapServers: String,
  // KEY_SERIALIZER_CLASS_CONFIG
  val keyDeserializer: Serializer<K>,
  // VALUE_SERIALIZER_CLASS_CONFIG
  val valueDeserializer: Serializer<V>,
  // ACKS_CONFIG
  val acks: Acks = Acks.One
) {
  public fun properties(): Properties =
    Properties().apply {
      put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      put(KEY_SERIALIZER_CLASS_CONFIG, keyDeserializer::class.qualifiedName)
      put(VALUE_SERIALIZER_CLASS_CONFIG, valueDeserializer::class.qualifiedName)
      put(ACKS_CONFIG, acks.value)
    }

  /**
   * // TODO Support Transactional behavior Kafka producer as a [Flow]. Will automatically close,
   * and flush when finished streaming. The [Flow] will close when the [KafkaProducer] is consumed
   * from the [Flow].
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
  public fun kafkaProducer(): Flow<KafkaProducer<K, V>> =
    kafkaProducer(properties(), keyDeserializer, valueDeserializer)
}

public fun <K, V> kafkaProducer(
  props: Properties,
  keyDeserializer: Serializer<K>,
  valueDeserializer: Serializer<V>
): Flow<KafkaProducer<K, V>> = flow {
  val producer = KafkaProducer(props, keyDeserializer, valueDeserializer)
  try {
    producer.use { emit(it) }
  } finally {
    producer.flush()
  }
}
