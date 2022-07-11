package io.github.nomisRev.kafka.consumer

import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.common.TopicPartition
import kotlinx.coroutines.flow.Flow
import io.github.nomisRev.kafka.ConsumerSettings

/**
 * Topic partition offset that must be acknowledged after the record in the corresponding [ReceiverRecord] is processed.
 */
public interface Offset {
  /** The topic partition corresponding to this [ReceiverRecord]. */
  public val topicPartition: TopicPartition
  
  /** The partition offset corresponding to the record to which this [ReceiverRecord] is associated. */
  public val offset: Long
  
  /**
   * Acknowledges the [ReceiverRecord] associated with this offset. The offset will be committed
   * automatically based on the commit configuration parameters [ConsumerSettings.commitInterval]
   * and [ConsumerSettings.commitBatchSize]. When an offset is acknowledged, it is assumed that
   * all records in this partition up to and including this offset have been processed.
   * All acknowledged offsets are committed if possible when the receiver [Flow] completes.
   */
  public suspend fun acknowledge(): Unit
  
  /**
   * Acknowledges the record associated with this instance and commits all acknowledged offsets.
   * This method suspends until the record has been committed,
   * it may be wrapped in `launch` to avoid suspending until commit has completed.
   *
   * If commit fails with [RetriableCommitFailedException]
   * the commit operation is retried [ConsumerSettings.maxCommitAttempts] times before this method returns.
   */
  public suspend fun commit(): Unit
}
