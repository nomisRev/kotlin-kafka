package io.github.nomisRev.kafka.receiver

import io.github.nomisRev.kafka.NothingDeserializer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer
import java.util.Properties
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

private val DEFAULT_POLL_TIMEOUT = 100.milliseconds
private const val DEFAULT_MAX_COMMIT_ATTEMPTS = 100
private val DEFAULT_COMMIT_RETRY_INTERVAL = 500.milliseconds
private val DEFAULT_COMMIT_INTERVAL = 5.seconds

/**
 * A data class that exposes configuration for [KafkaReceiver],
 * and the underlying [org.apache.kafka.clients.consumer.KafkaConsumer].
 *
 * It forces to specify the required parameters to offer a type-safe API,
 * so it requires [bootstrapServers], [valueDeserializer], and [groupId].
 * All other parameters are configured to the sanest defaults.
 *
 * @param createConsumer the way the underlying [Consumer] is created for [KafkaReceiver.receive]
 *   and [KafkaReceiver.receiveAutoAck]. Overriding it allows decorating the consumer, e.g. to
 *   customise its pause/resume behaviour, analogous to [io.github.nomisRev.kafka.publisher.PublisherSettings.createProducer].
 *   [KafkaReceiver.withConsumer] does not go through it: it hands out a [KafkaConsumer] rather than
 *   a [Consumer], so it cannot accept an arbitrary decoration without a breaking signature change.
 */
public data class ReceiverSettings<K, V>(
  val bootstrapServers: String,
  val keyDeserializer: Deserializer<K>,
  val valueDeserializer: Deserializer<V>,
  val groupId: String,
  val autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Earliest,
  val commitStrategy: CommitStrategy = CommitStrategy.ByTime(DEFAULT_COMMIT_INTERVAL),
  val pollTimeout: Duration = DEFAULT_POLL_TIMEOUT,
  val commitRetryInterval: Duration = DEFAULT_COMMIT_RETRY_INTERVAL,
  val maxCommitAttempts: Int = DEFAULT_MAX_COMMIT_ATTEMPTS,
  val maxDeferredCommits: Int = 0,
  val closeTimeout: Duration = Duration.INFINITE,
  val properties: Properties = Properties(),
  val createConsumer: suspend (ReceiverSettings<K, V>) -> Consumer<K, V> =
    { settings -> KafkaConsumer(settings.toProperties(), settings.keyDeserializer, settings.valueDeserializer) },
) {
  init {
    require(commitRetryInterval.isPosNonZero()) { "Commit Retry interval must be >= 0 but found $pollTimeout" }
    require(pollTimeout.isPosNonZero()) { "Poll timeout must be >= 0 but found $pollTimeout" }
    require(closeTimeout.isPosNonZero()) { "Close timeout must be >= 0 but found $closeTimeout" }
  }

  internal fun toProperties() = Properties().apply {
    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    if (keyDeserializer !== NothingDeserializer) {
      put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer::class.qualifiedName)
    }
    put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer::class.qualifiedName)
    put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.value)
    putAll(properties)
  }
}

/** Alternative constructor for [ReceiverSettings] without a key */
public fun <V> ReceiverSettings(
  bootstrapServers: String,
  valueDeserializer: Deserializer<V>,
  groupId: String,
  autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Earliest,
  commitStrategy: CommitStrategy = CommitStrategy.ByTime(DEFAULT_COMMIT_INTERVAL),
  pollTimeout: Duration = DEFAULT_POLL_TIMEOUT,
  commitRetryInterval: Duration = DEFAULT_COMMIT_RETRY_INTERVAL,
  maxCommitAttempts: Int = DEFAULT_MAX_COMMIT_ATTEMPTS,
  maxDeferredCommits: Int = 0,
  closeTimeout: Duration = Long.MAX_VALUE.nanoseconds,
  properties: Properties = Properties(),
  createConsumer: suspend (ReceiverSettings<Nothing, V>) -> Consumer<Nothing, V> =
    { settings -> KafkaConsumer(settings.toProperties(), settings.keyDeserializer, settings.valueDeserializer) },
): ReceiverSettings<Nothing, V> =
  ReceiverSettings(
    bootstrapServers = bootstrapServers,
    keyDeserializer = NothingDeserializer,
    valueDeserializer = valueDeserializer,
    groupId = groupId,
    autoOffsetReset = autoOffsetReset,
    commitStrategy = commitStrategy,
    pollTimeout = pollTimeout,
    commitRetryInterval = commitRetryInterval,
    maxCommitAttempts = maxCommitAttempts,
    maxDeferredCommits = maxDeferredCommits,
    closeTimeout = closeTimeout,
    properties = properties,
    createConsumer = createConsumer,
  )
