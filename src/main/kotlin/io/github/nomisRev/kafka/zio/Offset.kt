package io.github.nomisRev.kafka.zio

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.common.TopicPartition
import kotlin.math.max

public interface Offset {
  public val topicPartition: TopicPartition
  public val offset: Long
  public suspend fun commit(): Unit
  public val batch: OffsetBatch
  public val consumerGroupMetadata: ConsumerGroupMetadata?
  
  /**
   * Attempts to commit and retries according to the given policy when the commit fails with a
   * RetriableCommitFailedException
   */ // TODO expose as Arrow Fx extension?
  // def commitOrRetry[R](policy: Schedule[R, Throwable, Any]): RIO[R, Unit] =
  // Offset.commitOrRetry(commit, policy)
}

private data class OffsetImpl(
  override val topicPartition: TopicPartition,
  override val offset: Long,
  val commitHandle: suspend (Map<TopicPartition, Long>) -> Unit,
  override val consumerGroupMetadata: ConsumerGroupMetadata?,
) : Offset {
  override suspend fun commit(): Unit =
    commitHandle(mapOf(topicPartition to offset))
  
  override val batch: OffsetBatch =
    OffsetBatchImpl(mapOf(topicPartition to offset), commitHandle, consumerGroupMetadata)
}

public sealed interface OffsetBatch {
  public val offsets: Map<TopicPartition, Long>
  public suspend fun commit(): Unit
  public fun merge(offset: Offset): OffsetBatch
  public fun merge(offsets: OffsetBatch): OffsetBatch
  public val consumerGroupMetadata: ConsumerGroupMetadata?
  
  /**
   * Attempts to commit and retries according to the given policy when the commit fails with a
   * RetriableCommitFailedException
   */
  // def commitOrRetry[R](policy: Schedule[R, Throwable, Any]): RIO[R, Unit] =
  // Offset.commitOrRetry(commit, policy)
}

public fun emptyOffsetBatch(): OffsetBatch = EmptyOffsetBatch

public fun Iterable<Offset>.toBatch(): OffsetBatch = fold(emptyOffsetBatch()) { acc, offset ->
  acc.merge(offset)
}

private data class OffsetBatchImpl(
  override val offsets: Map<TopicPartition, Long>,
  val commitHandle: suspend (Map<TopicPartition, Long>) -> Unit,
  override val consumerGroupMetadata: ConsumerGroupMetadata?,
) : OffsetBatch {
  override suspend fun commit(): Unit = commitHandle(offsets)
  override fun merge(offset: Offset): OffsetBatch = copy(
    offsets = offsets + (offset.topicPartition to max(offsets.getOrElse(offset.topicPartition) { -1L }, offset.offset))
  )
  
  override fun merge(other: OffsetBatch): OffsetBatch = copy(
    offsets = buildMap {
      putAll(offsets)
      other.offsets.forEach { (tp, offset) ->
        val existing = offsets.getOrElse(tp) { -1L }
        if (existing < offset) put(tp, offset)
      }
    }
  )
}

public object EmptyOffsetBatch : OffsetBatch {
  override val offsets: Map<TopicPartition, Long> = emptyMap()
  override suspend fun commit(): Unit = Unit
  override fun merge(offset: Offset): OffsetBatch = offset.batch
  override fun merge(offsets: OffsetBatch): OffsetBatch = offsets
  override val consumerGroupMetadata: ConsumerGroupMetadata? = null
}