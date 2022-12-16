package io.github.nomisrev.kafka

import io.github.nomisrev.kafka.receiver.ReceiverSettings
import kotlin.time.Duration
import kotlinx.coroutines.flow.Flow

interface Consumer<K, V> {
  fun assignment(): Set<TopicPartition>
  fun subscription(): Set<String>
  fun subscribe(topics: Collection<String>)
  fun assign(partitions: Collection<TopicPartition>)
  fun unsubscribe()
  fun poll(timeout: Duration): List<ConsumerRecord<K, V>>
  fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>)
  fun commitAsync(
    offsets: Map<TopicPartition, OffsetAndMetadata>? = null,
    callback: ((Map<TopicPartition, OffsetAndMetadata>, Exception?) -> Unit)? = null
  )
  fun seek(partition: TopicPartition, offset: Long)
  fun seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata)
  fun seekToBeginning(partitions: Collection<TopicPartition>)
  fun seekToEnd(partitions: Collection<TopicPartition>)
  fun position(partition: TopicPartition, timeout: Duration? = null): Long
  fun committed(partitions: Set<TopicPartition>, timeout: Duration? = null): Map<TopicPartition, OffsetAndMetadata>
//  fun metrics(): Map<MetricName, Metric>
//  fun partitionsFor(topic: String, timeout: Duration? = null): List<PartitionInfo>
//  fun listTopics(timeout: Duration? = null): TopicsList
  fun paused(): Set<TopicPartition>
  fun pause(partitions: Collection<TopicPartition>)
  fun resume(partitions: Collection<TopicPartition>)
//  fun offsetsForTimes(
//    timestampsToSearch: Map<TopicPartition, Long>,
//    timeout: Duration? = null
//  ): Map<TopicPartition, OffsetAndTimestamp>
  fun beginningOffsets(partitions: Collection<TopicPartition>, timeout: Duration? = null): Map<TopicPartition, Long>
  fun endOffsets(partitions: Collection<TopicPartition>, timeout: Duration? = null): Map<TopicPartition, Long>
  fun currentLag(topicPartition: TopicPartition): Long?
//  fun groupMetadata(): ConsumerGroupMetadata
  fun enforceRebalance(reason: String? = null)
  fun close(timeout: Duration? = null)
  fun wakeup()
}

expect fun <K, V> kafkaConsumer(settings: ReceiverSettings<K, V>): Flow<Consumer<K, V>>
