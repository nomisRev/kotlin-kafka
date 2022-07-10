package io.github.nomisRev.kafka.reactor

import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.common.TopicPartition

/**
 * Topic partition offset that must be acknowledged after the record in the
 * corresponding [ReceiverRecord] is processed.
 *
 */
public interface ReceiverOffset {
  /**
   * Returns the topic partition corresponding to this instance.
   * @return topic partition
   */
  public fun topicPartition(): TopicPartition
  
  /**
   * Returns the partition offset corresponding to the record to which this instance is associated.
   * @return offset into partition
   */
  public fun offset(): Long
  
  /**
   * Acknowledges the [ReceiverRecord] associated with this offset. The offset will be committed
   * automatically based on the commit configuration parameters [ReceiverOptions.commitInterval]
   * and [ReceiverOptions.commitBatchSize]. When an offset is acknowledged, it is assumed that
   * all records in this partition up to and including this offset have been processed.
   * All acknowledged offsets are committed if possible when the receiver [Flux] terminates.
   */
  public fun acknowledge()
  
  /**
   * Acknowledges the record associated with this instance and commits all acknowledged offsets.
   *
   * This method commits asynchronously. [Mono.block] may be invoked on the returned Mono to
   * wait for completion of the commit. If commit fails with [RetriableCommitFailedException]
   * the commit operation is retried [ReceiverOptions.maxCommitAttempts] times before the
   * returned Mono is failed.
   * @return Mono that completes when commit operation completes.
   */
  public suspend fun commit(): Unit
}
