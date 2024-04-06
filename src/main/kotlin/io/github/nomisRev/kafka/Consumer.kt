@file:JvmName("Consumer")
@file:Suppress("DEPRECATION")

package io.github.nomisRev.kafka

import io.github.nomisRev.kafka.internal.chunked
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import java.time.Duration
import java.util.Properties
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.runInterruptible
import org.apache.kafka.clients.ClientDnsLookup
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.CHECK_CRCS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES
import org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.METADATA_MAX_AGE_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.RECEIVE_BUFFER_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.SEND_BUFFER_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.RangeAssignor
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

@Deprecated(
  "Use KafkaReceiver instead. This function will be removed in 1.0.0",
  ReplaceWith(
    "KafkaReceiver(settings.toReceiverSettings())",
    "io.github.nomisRev.kafka.receiver.KafkaReceiver"
  )
)
public fun <K, V> KafkaConsumer(settings: ConsumerSettings<K, V>): KafkaConsumer<K, V> =
  KafkaConsumer(settings.properties(), settings.keyDeserializer, settings.valueDeserializer)

@Deprecated(
  "Use KafkaReceiver#receive instead. This function will be removed in 1.0.0",
  ReplaceWith(
    "KafkaReceiver(settings.toReceiverSettings()).receive()",
    "io.github.nomisRev.kafka.receiver.KafkaReceiver"
  )
)
public fun <K, V> kafkaConsumer(settings: ConsumerSettings<K, V>): Flow<KafkaConsumer<K, V>> =
  flow {
    KafkaConsumer(settings).use {
      emit(it)
    }
  }

/**
 * Commits offsets in batches of every [count] offsets or a certain [duration] has passed, whichever happens first.
 */
@OptIn(FlowPreview::class)
@ExperimentalCoroutinesApi
@JvmName("commitBatchesWithin")
@Deprecated("Use KafkaReceiver instead. It comes with strong guarantees about commits")
public fun <K, V> Flow<ConsumerRecords<K, V>>.commitBatchWithin(
  settings: ConsumerSettings<K, V>,
  count: Int,
  duration: kotlin.time.Duration,
  metadata: ((record: ConsumerRecord<K, V>) -> String)? = null,
): Flow<Map<TopicPartition, OffsetAndMetadata>> = kafkaConsumer(settings).flatMapConcat { consumer ->
  chunked(count, duration).mapNotNull { records ->
    if (records.isNotEmpty()) consumer.commitAwait(records.offsets(metadata))
    else null
  }
}

@OptIn(FlowPreview::class)
@ExperimentalCoroutinesApi
@Deprecated("Use KafkaReceiver instead. It comes with strong guarantees about commits")
public fun <K, V> Flow<ConsumerRecord<K, V>>.commitBatchWithin(
  settings: ConsumerSettings<K, V>,
  count: Int,
  duration: kotlin.time.Duration,
  metadata: ((record: ConsumerRecord<K, V>) -> String)? = null,
): Flow<Unit> = kafkaConsumer(settings).flatMapConcat { consumer ->
  chunked(count, duration).mapNotNull { records ->
    val commitAsyncMap = records.offsets(metadata)
    (if (records.isNotEmpty()) consumer.commitAsync(commitAsyncMap) { _, e ->
      if (e != null) throw e
    } else Unit)
  }
}

@Deprecated(
  "Use KafkaReceiver instead. It comes with strong guarantees about commits." +
    "You can only commit while polling, which is done automatically for you with KafkaReceiver"
)
public suspend fun <K, V> KafkaConsumer<K, V>.commitAwait(
  offsets: Map<TopicPartition, OffsetAndMetadata>,
): Map<TopicPartition, OffsetAndMetadata> =
  suspendCoroutine { cont ->
    commitAsync(offsets) { offsets, e ->
      if (e != null) cont.resumeWithException(e)
      else cont.resume(offsets)
    }
  }

public operator fun <K, V> ConsumerRecord<K, V>.component1(): K = key()
public operator fun <K, V> ConsumerRecord<K, V>.component2(): V = value()

public fun <K, V> Iterable<ConsumerRecord<K, V>>.offsets(
  metadata: ((record: ConsumerRecord<K, V>) -> String)? = null,
): Map<TopicPartition, OffsetAndMetadata> =
  mutableMapOf<TopicPartition, OffsetAndMetadata>().apply {
    this@offsets.forEach { record ->
      val key = TopicPartition(record.topic(), record.partition())
      val value = metadata?.let {
        OffsetAndMetadata(record.offset() + 1, record.leaderEpoch(), metadata(record))
      } ?: OffsetAndMetadata(record.offset() + 1)
      put(key, value)
    }
  }

public fun <K, V> ConsumerRecord<K, V>.offsets(
  metadata: ((record: ConsumerRecord<K, V>) -> String)? = null,
): Map<TopicPartition, OffsetAndMetadata> = buildMap {
  val key = TopicPartition(topic(), partition())
  val value = metadata?.let {
    OffsetAndMetadata(offset() + 1, leaderEpoch(), metadata(this@offsets))
  } ?: OffsetAndMetadata(offset() + 1)
  put(key, value)
}

@JvmName("offsetsBatches")
public fun <K, V> List<ConsumerRecords<K, V>>.offsets(
  metadata: ((record: ConsumerRecord<K, V>) -> String)? = null,
): Map<TopicPartition, OffsetAndMetadata> = buildMap {
  this@offsets.forEach {
    putAll(it.offsets(metadata))
  }
}

@Deprecated(
  "Use KafkaReceiver#receive instead. This function will be removed in 1.0.0",
  ReplaceWith(
    "KafkaReceiver(settings.toReceiverSettings()).receive()",
    "io.github.nomisRev.kafka.receiver.KafkaReceiver"
  )
)
@OptIn(ExperimentalCoroutinesApi::class)
public fun <K, V> Flow<KafkaConsumer<K, V>>.subscribeTo(
  name: String,
  dispatcher: CoroutineDispatcher = IO,
  listener: ConsumerRebalanceListener = NoOpConsumerRebalanceListener,
  timeout: kotlin.time.Duration = 500.milliseconds,
): Flow<ConsumerRecord<K, V>> = flatMapConcat { consumer ->
  consumer.subscribeTo(name, dispatcher, listener, timeout)
}

@Deprecated(
  "Use KafkaReceiver#receive instead. This function will be removed in 1.0.0",
  ReplaceWith(
    "KafkaReceiver(settings.toReceiverSettings()).receive()",
    "io.github.nomisRev.kafka.receiver.KafkaReceiver"
  )
)
/** Subscribes to the [KafkaConsumer] and polls for events in an interruptible way. */
public fun <K, V> KafkaConsumer<K, V>.subscribeTo(
  name: String,
  dispatcher: CoroutineDispatcher = IO,
  listener: ConsumerRebalanceListener = NoOpConsumerRebalanceListener,
  timeout: kotlin.time.Duration = 500.milliseconds,
): Flow<ConsumerRecord<K, V>> = flow {
  subscribe(listOf(name), listener)
  val job: Job? = coroutineContext[Job]
  while (true) {
    job?.ensureActive()
    // KotlinX Coroutines catches java.util.InterruptedException,
    // but Kafka uses its own runtime with org.apache.kafka.common.errors.InterruptException
    try {
      runInterruptible(dispatcher) {
        poll(timeout.toJavaDuration())
      }.let { emitAll(it.asFlow()) }
    } catch (e: InterruptException) {
      throw CancellationException("Blocking call was interrupted due to parent cancellation").initCause(e)
    }
  }
}

@Deprecated(
  "Use io.github.nomisRev.kafka.receiver.AutoOffsetReset instead",
  ReplaceWith(
    "this",
    "io.github.nomisRev.kafka.receiver.AutoOffsetReset"
  )
)
public typealias AutoOffsetReset =
  io.github.nomisRev.kafka.receiver.AutoOffsetReset

/** Default values taken from [org.apache.kafka.clients.consumer.ConsumerConfig] */
@Deprecated(
  "Use ReceiverSettings with KafkaReceiver instead.",
  ReplaceWith("toReceiverSettings()")
)
public data class ConsumerSettings<K, V>(
  // BOOTSTRAP_SERVERS_CONFIG
  val bootstrapServers: String,
  // KEY_DESERIALIZER_CLASS_CONFIG
  val keyDeserializer: Deserializer<K>,
  // VALUE_DESERIALIZER_CLASS_CONFIG
  val valueDeserializer: Deserializer<V>,
  // GROUP_ID_CONFIG
  val groupId: String,
  // CLIENT_DNS_LOOKUP_CONFIG
  val clientDnsLookup: ClientDnsLookup = ClientDnsLookup.USE_ALL_DNS_IPS,
  // SESSION_TIMEOUT_MS_CONFIG
  val sessionTimeOut: Duration = Duration.ofMillis(10000),
  // HEARTBEAT_INTERVAL_MS_CONFIG
  val heartbeatInterval: Duration = Duration.ofMillis(3000),
  // AUTO_OFFSET_RESET_CONFIG
  val autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Latest,
  // PARTITION_ASSIGNMENT_STRATEGY_CONFIG
  val partitionAssignmentStrategy: List<Class<*>> = listOf(RangeAssignor::class.java),
  // METADATA_MAX_AGE_CONFIG (AT LEAST 0)
  val metadataMaxAge: Long = (5 * 60 * 1000).toLong(),
  // ENABLE_AUTO_COMMIT_CONFIG
  val enableAutoCommit: Boolean = true,
  // AUTO_COMMIT_INTERVAL_MS_CONFIG
  val autoCommitInterval: Duration = Duration.ofMillis(5000),
  // CLIENT_ID_CONFIG
  val clientId: String = "",
  // MAX_PARTITION_FETCH_BYTES_CONFIG (AT LEAST 0)
  val maxPartitionFetchBytes: Int = DEFAULT_MAX_PARTITION_FETCH_BYTES,
  // SEND_BUFFER_CONFIG
  val sendBuffer: Int = 128 * 1024,
  // RECEIVE_BUFFER_CONFIG (AT LEAST [CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND])
  val receiveBuffer: Int = 64 * 1024,
  // FETCH_MIN_BYTES_CONFIG (AT LEAST 0)
  val fetchMinBytes: Int = 1,
  // FETCH_MAX_BYTES_CONFIG (AT LEAST 0)
  val fetchMaxBytes: Int = ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
  // FETCH_MAX_WAIT_MS_CONFIG (AT LEAST 0)
  val fetchMaxWait: Duration = Duration.ofMillis(500),
  // RECONNECT_BACKOFF_MS_CONFIG (AT LEAST 0)
  val reconnectBackoff: Duration = Duration.ofMillis(50L),
  // RECONNECT_BACKOFF_MAX_MS_CONFIG
  val reconnectBackoffMax: Duration = Duration.ofMillis(1000),
  // RETRY_BACKOFF_MS_CONFIG
  val retryBackoff: Duration = Duration.ofMillis(100),
  // CHECK_CRCS_CONFIG
  val checkCrcs: Boolean = true,
  // METRICS_SAMPLE_WINDOW_MS_CONFIG (AT LEAST 0)
  val metricsSampleWindow: Duration = Duration.ofMillis(30000),
  // METRICS_NUM_SAMPLES_CONFIG (AT LEAST 1)
  val metricsNumSamples: Int = 2,
  // METRICS_RECORDING_LEVEL_CONFIG
  val metricsRecordingLevel: Sensor.RecordingLevel = Sensor.RecordingLevel.INFO,
  // METRIC_REPORTER_CLASSES_CONFIG
  val metricsReporterClasses: List<Class<*>> = emptyList(),
  // REQUEST_TIMEOUT_MS_CONFIG (AT LEAST 0)
  val requestTimeout: Duration = Duration.ofMillis(30000),
  // DEFAULT_API_TIMEOUT_MS_CONFIG (AT LEAST 0)
  val defaultApiTimeout: Duration = Duration.ofMillis(60 * 1000),
  // CONNECTIONS_MAX_IDLE_MS_CONFIG
  val connectionsMaxIdle: Duration = Duration.ofMillis(9 * 60 * 1000),
  // INTERCEPTOR_CLASSES_CONFIG
  val interceptorClasses: List<Class<*>> = emptyList(),
  // MAX_POLL_RECORDS_CONFIG (AT LEAST 1)
  val maxPollRecords: Int = 500,
  // MAX_POLL_INTERVAL_MS_CONFIG (AT LEAST 1)
  val maxPollInterval: Duration = Duration.ofMillis(300_000),
  // EXCLUDE_INTERNAL_TOPICS_CONFIG
  val excludeInternalTopics: Boolean = ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS,
  // Optional parameter that allows for setting properties not defined here
  private val properties: Properties? = null,
) {
  public fun toReceiverSettings(): ReceiverSettings<K, V> =
    ReceiverSettings(
      bootstrapServers,
      keyDeserializer,
      valueDeserializer,
      groupId,
      properties = properties()
    )

  public fun properties(): Properties = Properties().apply {
    properties?.let { putAll(it) }
    put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    put(AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.value)
    put(GROUP_ID_CONFIG, groupId)
    put(CLIENT_DNS_LOOKUP_CONFIG, clientDnsLookup.toString())
    put(SESSION_TIMEOUT_MS_CONFIG, sessionTimeOut.toMillis().toInt())
    put(HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval.toMillis().toInt())
    put(AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.value)
    put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy.joinToString(separator = ",") { it.name })
    put(METADATA_MAX_AGE_CONFIG, metadataMaxAge)
    put(ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit)
    put(AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval.toMillis().toInt())
    put(CLIENT_ID_CONFIG, clientId)
    put(MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes)
    put(SEND_BUFFER_CONFIG, sendBuffer)
    put(RECEIVE_BUFFER_CONFIG, receiveBuffer)
    put(FETCH_MIN_BYTES_CONFIG, fetchMinBytes)
    put(FETCH_MAX_BYTES_CONFIG, fetchMaxBytes)
    put(RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoff.toMillis().toInt())
    put(RECONNECT_BACKOFF_MAX_MS_CONFIG, reconnectBackoffMax.toMillis().toInt())
    put(RETRY_BACKOFF_MS_CONFIG, retryBackoff.toMillis().toInt())
    put(CHECK_CRCS_CONFIG, checkCrcs)
    put(METRICS_SAMPLE_WINDOW_MS_CONFIG, metricsSampleWindow.toMillis().toInt())
    put(METRICS_NUM_SAMPLES_CONFIG, metricsNumSamples)
    put(METRICS_RECORDING_LEVEL_CONFIG, metricsRecordingLevel.toString())
    put(METRIC_REPORTER_CLASSES_CONFIG, metricsReporterClasses.joinToString(separator = ",") { it.name })
    put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer::class.qualifiedName)
    put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer::class.qualifiedName)
    put(REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toMillis().toInt())
    put(DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeout.toMillis().toInt())
    put(CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdle.toMillis().toInt())
    put(INTERCEPTOR_CLASSES_CONFIG, interceptorClasses.joinToString(separator = ",") { it.name })
    put(MAX_POLL_RECORDS_CONFIG, maxPollRecords)
    put(MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval.toMillis().toInt())
    put(EXCLUDE_INTERNAL_TOPICS_CONFIG, excludeInternalTopics)
  }
}

private object NoOpConsumerRebalanceListener : ConsumerRebalanceListener {
  override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {}
  override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {}
}