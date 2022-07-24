package io.github.nomisRev.kafka.receiver

import org.apache.kafka.common.TopicPartition

/**
 * Topic partition interface that supports `seek` operations that can be invoked when partitions are assigned.
 */
public interface ConsumerPartition {
  
  /** Returns the underlying Kafka topic partition. */
  public val topicPartition: TopicPartition
  
  /**
   * Seeks to the first available offset of the topic partition.
   * This overrides the offset starting from which records are fetched.
   */
  public fun seekToBeginning()
  
  /**
   * Seeks to the last offset of the topic partition.
   * This overrides the offset starting from which records are fetched.
   */
  public fun seekToEnd()
  
  /**
   * Seeks to the specified offset of the topic partition.
   * This overrides the offset starting from which records are fetched.
   */
  public fun seek(offset: Long)
  
  /**
   * Seek to the topic partition offset that is greater than or equal to the timestamp.
   * If there are no matching records, [seekToEnd] is performed.
   * See [org.apache.kafka.clients.consumer.Consumer.offsetsForTimes].
   */
  public fun seekToTimestamp(timestamp: Long)
  
  /**
   * Returns the offset of the next record that will be fetched from this topic partition.
   * @return current offset of this partition
   */
  public fun position(): Long
}
