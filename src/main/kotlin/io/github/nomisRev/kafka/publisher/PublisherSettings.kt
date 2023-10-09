package io.github.nomisRev.kafka.publisher

import io.github.nomisRev.kafka.NothingSerializer
import io.github.nomisRev.kafka.publisher.PublisherSettings.ProducerListener
import io.github.nomisRev.kafka.receiver.isPosNonZero
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.serialization.Serializer
import java.util.Properties
import kotlin.time.Duration

/**
 * Typed setting to create a [Publisher], enforces the required parameters and leaves the rest as [Properties].
 *
 * @param bootstrapServers list of host/port pairs to use for establishing the initial connection to the Kafka cluster. Should be comma separated.
 * @param keySerializer the [Serializer] to use to serialize the [Key] when sending messages to kafka.
 * @param valueSerializer the [Serializer] to use to serialize the [Value] when sending messages to kafka.
 * @param acknowledgments configuration to use.
 * @param closeTimeout the timeout when closing the created underlying [Producer], default [Duration.INFINITE].
 * @param producerListener listener that is called whenever a [Producer] is added, and removed.
 */
data class PublisherSettings<Key, Value>(
  val bootstrapServers: String,
  val keySerializer: Serializer<Key>,
  val valueSerializer: Serializer<Value>,
  val acknowledgments: Acks = Acks.One,
  val closeTimeout: Duration = Duration.INFINITE,
  val isFatal: (t: Throwable) -> Boolean =
    { it is AuthenticationException || it is ProducerFencedException },
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

  fun transactionalId(): String? =
    properties[ProducerConfig.TRANSACTIONAL_ID_CONFIG] as? String

  /**
   * Senders created from this options will be transactional if a transactional id is
   * configured using {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG}. If transactional,
   * {@link KafkaProducer#initTransactions()} is invoked on the producer to initialize
   * transactions before any operations are performed on the sender. If scheduler is overridden
   * using {@link #scheduler(Scheduler)}, the configured scheduler
   * must be single-threaded. Otherwise, the behaviour is undefined and may result in unexpected
   * exceptions.
   */
  fun isTransactional(): Boolean =
    !transactionalId().isNullOrBlank()

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

/** Alternative constructor for [PublisherSettings] without a key */
public fun <Value> PublisherOptions(
  bootstrapServers: String,
  valueSerializer: Serializer<Value>,
  acknowledgments: Acks = Acks.One,
  closeTimeout: Duration = Duration.INFINITE,
  isFatal: (t: Throwable) -> Boolean =
    { it is AuthenticationException || it is ProducerFencedException },
  producerListener: ProducerListener = NoOpProducerListener,
  properties: Properties = Properties(),
): PublisherSettings<Nothing, Value> =
  PublisherSettings(
    bootstrapServers,
    NothingSerializer,
    valueSerializer,
    acknowledgments,
    closeTimeout,
    isFatal,
    producerListener,
    properties
  )

private object NoOpProducerListener : ProducerListener
