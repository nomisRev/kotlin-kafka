package io.github.nomisrev.kafka

import com.icemachined.kafka.clients.consumer.Consumer as KafkaConsumer
import com.icemachined.kafka.common.TopicPartition as KafkaTopicPartition
import com.icemachined.kafka.clients.consumer.OffsetAndMetadata as KafkaOffsetAndMetadata
import com.icemachined.kafka.clients.consumer.OffsetCommitCallback
import io.github.nomisrev.kafka.receiver.ReceiverSettings
import kotlin.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

actual fun <K, V> kafkaConsumer(settings: ReceiverSettings<K, V>): Flow<Consumer<K, V>> =
  flow {
    val consumer = com.icemachined.kafka.clients.consumer.KafkaConsumer(
      settings.toPropertyMap(),
      settings.keyDeserializer.toNativeDeserializer(),
      settings.valueDeserializer.toNativeDeserializer()
    )
    try {
      emit(ConsumerWrapper(consumer))
    } finally {
      consumer.close()
    }
  }

fun TopicPartition.toKafka(): KafkaTopicPartition =
  KafkaTopicPartition(topic, partition)

fun OffsetAndMetadata.toKafka(): KafkaOffsetAndMetadata =
  KafkaOffsetAndMetadata(offset.toULong(), leaderEpoch, metadata)

fun KafkaTopicPartition.toCommon(): TopicPartition =
  TopicPartition(topic, partition)

fun KafkaOffsetAndMetadata.toCommon(): OffsetAndMetadata =
  OffsetAndMetadata(offset.toLong(), leaderEpoch, metadata)

fun Map<KafkaTopicPartition, KafkaOffsetAndMetadata>.toCommon(): Map<TopicPartition, OffsetAndMetadata> =
  buildMap(size) {
    this@toCommon.forEach { (tp, offset) -> put(tp.toCommon(), offset.toCommon()) }
  }

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
    delegate.poll(timeout).map {
      ConsumerRecord(
        topic = it.topic,
        partition = it.partition,
        offset = it.offset.toLong(),
        timestamp = it.timestamp,
        timestampType = it.timestampType,
        serializedKeySize = it.serializedKeySize,
        serializedValueSize = it.serializedValueSize,
        key = it.key,
        value = it.value,
        headers = it.headers,
        leaderEpoch = it.leaderEpoch
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
  ) {
    val map = offsets?.let {
      buildMap(offsets.size) {
        offsets.forEach { (tp, offset) ->
          put(tp.toKafka(), offset.toKafka())
        }
      }
    }
    val offsetCommitCallback = callback?.let {
      object : OffsetCommitCallback {
        override fun onComplete(
          offsets: Map<com.icemachined.kafka.common.TopicPartition, com.icemachined.kafka.clients.consumer.OffsetAndMetadata>,
          exception: Exception?
        ) = callback(offsets.toCommon(), exception)
      }
    }
    delegate.commitAsync(map, offsetCommitCallback)
  }

  override fun seek(partition: TopicPartition, offset: Long) =
    delegate.seek(partition.toKafka(), offset)

  override fun seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) =
    delegate.seek(partition.toKafka(), offsetAndMetadata.toKafka())

  override fun seekToBeginning(partitions: Collection<TopicPartition>) =
    delegate.seekToBeginning(partitions.toKafka())

  override fun seekToEnd(partitions: Collection<TopicPartition>) =
    delegate.seekToEnd(partitions.toKafka())

  override fun position(partition: TopicPartition, timeout: Duration?): Long =
    delegate.position(partition.toKafka(), timeout)

  override fun committed(
    partitions: Set<TopicPartition>,
    timeout: Duration?
  ): Map<TopicPartition, OffsetAndMetadata> =
    delegate.committed(partitions.toKafka(), timeout).toCommon()

  override fun paused(): Set<TopicPartition> =
    delegate.paused().toCommon()

  override fun pause(partitions: Collection<TopicPartition>) =
    delegate.pause(partitions.toKafka())

  override fun resume(partitions: Collection<TopicPartition>) =
    delegate.resume(partitions.toKafka())

  override fun beginningOffsets(
    partitions: Collection<TopicPartition>,
    timeout: Duration?
  ): Map<TopicPartition, Long> =
    delegate.beginningOffsets(partitions.toKafka(), timeout).toCommon()

  override fun endOffsets(partitions: Collection<TopicPartition>, timeout: Duration?): Map<TopicPartition, Long> =
    delegate.endOffsets(partitions.toKafka(), timeout).toCommon()

  override fun currentLag(topicPartition: TopicPartition): Long =
    delegate.currentLag(topicPartition.toKafka())

  override fun enforceRebalance(reason: String?) =
    delegate.enforceRebalance(reason)

  override fun close(timeout: Duration?) =
    if (timeout == null) delegate.close() else delegate.close(timeout)

  override fun wakeup() =
    delegate.wakeup()
}
