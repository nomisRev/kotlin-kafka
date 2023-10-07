package io.github.nomisRev.kafka.publisher

import io.github.nomisRev.kafka.NothingSerializer
import io.github.nomisRev.kafka.receiver.isPosNonZero
import io.github.nomisRev.kafka.publisher.PublisherOptions.ProducerListener
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer
import java.util.Properties
import kotlin.time.Duration

/**
 * @param bootstrapServers list of host/port pairs to use for establishing the initial connection to the Kafka cluster. Should be comma separated.
 * @param keySerializer the [Serializer] to use to serialize the [Key] when sending messages to kafka.
 * @param valueSerializer the [Serializer] to use to serialize the [Value] when sending messages to kafka.
 * @param acknowledgments configuration to use.
 * @param closeTimeout the timeout when closing the created underlying [Producer], default [Duration.INFINITE].
 * @param maxInFlight the maximum number of in-flight records that are fetched from the outbound record publisher while acknowledgements are pending. Default [Channel.BUFFERED].
 * @param stopOnError if a send operation should be terminated when an error is encountered. If set to false, send is attempted for all records in a sequence even if send of one of the records fails with a non-fatal exception.
 * @param producerListener listener that is called whenever a [Producer] is added, and removed.
 */
data class PublisherOptions<Key, Value>(
  val bootstrapServers: String,
  val keySerializer: Serializer<Key>,
  val valueSerializer: Serializer<Value>,
  val acknowledgments: Acks = Acks.One,
  val closeTimeout: Duration = Duration.INFINITE,
  val maxInFlight: Int = Channel.BUFFERED,
  val stopOnError: Boolean = true,
  val producerListener: ProducerListener = NoOpProducerListener,
  val properties: Properties = Properties(),
) {

  init {
    require(closeTimeout.isPosNonZero()) { "Close timeout must be >= 0 but found $closeTimeout" }
  }

  internal fun properties(): Properties = Properties().apply {
    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer::class.qualifiedName)
    put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer::class.qualifiedName)
    put(ProducerConfig.ACKS_CONFIG, acknowledgments.value)
    putAll(properties)
  }

  /** Called whenever a [Producer] is added or removed. */
  interface ProducerListener {
    /**
     * A new producer was created.
     * @param id the producer id (factory name and client.id separated by a period).
     * @param producer the producer.
     */
    fun producerAdded(id: String, producer: Producer<*, *>) {}

    /**
     * An existing producer was removed.
     * @param id the producer id (factory bean name and client.id separated by a period).
     * @param producer the producer.
     */
    fun producerRemoved(id: String, producer: Producer<*, *>) {}
  }
}

/** Alternative constructor for [PublisherOptions] without a key */
public fun <Value> PublisherOptions(
  bootstrapServers: String,
  valueSerializer: Serializer<Value>,
  acknowledgments: Acks = Acks.One,
  closeTimeout: Duration = Duration.INFINITE,
  maxInFlight: Int = Channel.BUFFERED,
  stopOnError: Boolean = true,
  producerListener: ProducerListener = NoOpProducerListener,
  properties: Properties = Properties(),
): PublisherOptions<Nothing, Value> =
  PublisherOptions(
    bootstrapServers,
    NothingSerializer,
    valueSerializer,
    acknowledgments,
    closeTimeout,
    maxInFlight,
    stopOnError,
    producerListener,
    properties
  )

private object NoOpProducerListener : ProducerListener
