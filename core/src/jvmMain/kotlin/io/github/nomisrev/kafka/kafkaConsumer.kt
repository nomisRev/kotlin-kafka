package io.github.nomisrev.kafka

import org.apache.kafka.clients.consumer.Consumer as KafkaConsumer
import org.apache.kafka.common.TopicPartition as KafkaTopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata as KafkaOffsetAndMetadata
import io.github.nomisrev.kafka.receiver.NothingDeserializer
import io.github.nomisrev.kafka.receiver.ReceiverSettings
import java.util.Optional
import java.util.Properties
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

actual fun <K, V> kafkaConsumer(settings: ReceiverSettings<K, V>): Flow<Consumer<K, V>> =
  flow {
    org.apache.kafka.clients.consumer.KafkaConsumer(
      settings.properties(),
      settings.keyDeserializer.toJavaDeserializer(),
      settings.valueDeserializer.toJavaDeserializer()
    ).use { emit(ConsumerWrapper(it)) }
  }

fun <K, V> ReceiverSettings<K, V>.properties() = Properties().apply {
  put(CommonConfigNames.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  if (keyDeserializer !== NothingDeserializer) {
    put(CommonConfigNames.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer::class.qualifiedName)
  }
  put(CommonConfigNames.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer::class.qualifiedName)
  put(CommonConfigNames.ENABLE_AUTO_COMMIT_CONFIG, "false")
  put(CommonConfigNames.GROUP_ID_CONFIG, groupId)
  put(CommonConfigNames.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.value)
  putAll(properties)

  // TODO: call configure() on the deserializers
}

fun TopicPartition.toKafka(): KafkaTopicPartition =
  KafkaTopicPartition(topic, partition)

fun OffsetAndMetadata.toKafka(): KafkaOffsetAndMetadata =
  KafkaOffsetAndMetadata(offset, Optional.ofNullable(leaderEpoch), metadata)

fun KafkaTopicPartition.toCommon(): TopicPartition =
  TopicPartition(topic(), partition())

fun KafkaOffsetAndMetadata.toCommon(): OffsetAndMetadata =
  OffsetAndMetadata(offset(), leaderEpoch().orElse(null), metadata())

fun Map<KafkaTopicPartition, KafkaOffsetAndMetadata>.toCommon(): Map<TopicPartition, OffsetAndMetadata> =
  buildMap(size) {
    this@toCommon.forEach { (tp, offset) -> put(tp.toCommon(), offset.toCommon()) }
  }

@JvmName("toCommonLong")
fun Map<KafkaTopicPartition, Long>.toCommon(): Map<TopicPartition, Long> =
  buildMap(size) {
    this@toCommon.forEach { (tp, offset) -> put(tp.toCommon(), offset) }
  }

fun Set<KafkaTopicPartition>.toCommon(): Set<TopicPartition> =
  buildSet(size) {
    this@toCommon.forEach { add(it.toCommon()) }
  }

fun Set<TopicPartition>.toKafka(): Set<KafkaTopicPartition> =
  buildSet(size) {
    this@toKafka.forEach { add(it.toKafka()) }
  }

fun Collection<TopicPartition>.toKafka(): List<KafkaTopicPartition> =
  map { it.toKafka() }

@JvmInline
private value class ConsumerWrapper<K, V>(
  val delegate: KafkaConsumer<K, V>
) : Consumer<K, V> {
  override fun assignment(): Set<TopicPartition> = buildSet {
    delegate.assignment().forEach { tp -> add(tp.toCommon()) }
  }

  override fun subscription(): Set<String> =
    delegate.subscription()

  override fun subscribe(topics: Collection<String>) =
    delegate.subscribe(topics)

  override fun assign(partitions: Collection<TopicPartition>) =
    delegate.assign(partitions.toKafka())

  override fun unsubscribe() =
    delegate.unsubscribe()

  override fun poll(timeout: Duration): List<ConsumerRecord<K, V>> =
    delegate.poll(timeout.toJavaDuration()).map {
      ConsumerRecord(
        topic = it.topic(),
        partition = it.partition(),
        offset = it.offset(),
        timestamp = it.timestamp(),
        timestampType = it.timestampType(),
        serializedKeySize = it.serializedKeySize(),
        serializedValueSize = it.serializedValueSize(),
        key = it.key(),
        value = it.value(),
        headers = it.headers().toHeaders(),
        leaderEpoch = it.leaderEpoch().orElse(null)
      )
    }

  override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>) =
    delegate.commitSync(
      buildMap(offsets.size) {
        offsets.forEach { (tp, offset) ->
          put(tp.toKafka(), offset.toKafka())
        }
      }
    )

  override fun commitAsync(
    offsets: Map<TopicPartition, OffsetAndMetadata>?,
    callback: ((Map<TopicPartition, OffsetAndMetadata>, Exception?) -> Unit)?
  ) =
    if (offsets == null && callback == null) delegate.commitAsync()
    else if (offsets == null && callback != null) delegate.commitAsync { tp, e -> callback(tp.toCommon(), e) }
    else delegate.commitAsync(buildMap(offsets?.size ?: 0) {
      offsets?.forEach { (tp, offset) ->
        put(tp.toKafka(), offset.toKafka())
      }
    }) { tp, e -> callback?.invoke(tp.toCommon(), e) }

  override fun seek(partition: TopicPartition, offset: Long) =
    delegate.seek(partition.toKafka(), offset)

  override fun seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) =
    delegate.seek(partition.toKafka(), offsetAndMetadata.toKafka())

  override fun seekToBeginning(partitions: Collection<TopicPartition>) =
    delegate.seekToBeginning(partitions.toKafka())

  override fun seekToEnd(partitions: Collection<TopicPartition>) =
    delegate.seekToEnd(partitions.toKafka())

  override fun position(partition: TopicPartition, timeout: Duration?): Long =
    delegate.position(partition.toKafka(), timeout?.toJavaDuration())

  override fun committed(
    partitions: Set<TopicPartition>,
    timeout: Duration?
  ): Map<TopicPartition, OffsetAndMetadata> =
    delegate.committed(partitions.toKafka(), timeout?.toJavaDuration()).toCommon()

  override fun paused(): Set<TopicPartition> =
    delegate.paused().toCommon()

  override fun pause(partitions: Collection<TopicPartition>) =
    delegate.pause(partitions.toKafka())

  override fun resume(partitions: Collection<TopicPartition>) =
    delegate.resume(partitions.toKafka())

  override fun beginningOffsets(partitions: Collection<TopicPartition>, timeout: Duration?): Map<TopicPartition, Long> =
    delegate.beginningOffsets(partitions.toKafka(), timeout?.toJavaDuration()).toCommon()

  override fun endOffsets(partitions: Collection<TopicPartition>, timeout: Duration?): Map<TopicPartition, Long> =
    delegate.endOffsets(partitions.toKafka(), timeout?.toJavaDuration()).toCommon()

  override fun currentLag(topicPartition: TopicPartition): Long? {
    val optional = delegate.currentLag(topicPartition.toKafka())
    return if (optional.isPresent) optional.asLong else null
  }

  override fun enforceRebalance(reason: String?) =
    delegate.enforceRebalance(reason)

  override fun close(timeout: Duration?) =
    if (timeout == null) delegate.close() else delegate.close(timeout.toJavaDuration())

  override fun wakeup() =
    delegate.wakeup()
}
