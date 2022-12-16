package io.github.nomisrev.kafka.receiver

import io.github.nomisrev.kafka.TopicPartition

/**
 * Topic partition offset that must be acknowledged after the record is processed.
 *
 * When you [acknowledge] this [Offset]  it will be added to the batch of offsets to be committed based on [CommitStrategy],
 * whilst [commit] will actually commit this commit to kafka, and suspend until it's completed.
 *
 * So if you want to _force_ a commit without back-pressuring a stream, you can use `launch { offset.commit() }`.
 * This is considered a niche use-case,
 * and using the batch commit functionality with [CommitStrategy] and [acknowledge] is recommended.
 */
public interface Offset {
  /** The topic partition corresponding to this [ReceiverRecord]. */
  public val topicPartition: TopicPartition

  /** The partition offset corresponding to the record to which this [ReceiverRecord] is associated. */
  public val offset: Long

  /**
   * Acknowledges the [ReceiverRecord] associated with this offset.
   * The offset will be committed automatically based on the commit configuration [CommitStrategy].
   * When an offset is acknowledged, it is assumed that all records in this partition up to,
   * and including this offset have been processed. All acknowledged offsets are committed if possible
   * when the receiver [Flow] completes or according to [CommitStrategy].
   */
  public suspend fun acknowledge(): Unit

  /**
   * Acknowledges the record associated with this instance and commits all acknowledged offsets.
   * This method suspends until the record has been committed,
   * it may be wrapped in `launch` to avoid suspending until commit has completed.
   *
   * If commit fails with [RetriableCommitFailedException]
   * the commit operation is retried [ReceiverSettings.maxCommitAttempts] times before this method returns.
   */
  public suspend fun commit(): Unit
}
