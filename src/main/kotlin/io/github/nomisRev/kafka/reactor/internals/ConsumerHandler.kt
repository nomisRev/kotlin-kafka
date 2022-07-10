package io.github.nomisRev.kafka.reactor.internals

import io.github.nomisRev.kafka.ConsumerSettings
import io.github.nomisRev.kafka.KafkaConsumer
import io.github.nomisRev.kafka.reactor.ReceiverOffset
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flow
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.common.TopicPartition
import java.util.Arrays
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A helper class that holds the state of a current receive "session".
 * To be exposed as a public class in the next major version (a subject to the API review).
 */
internal class ConsumerHandler<K, V>(
  private val receiverOptions: ConsumerSettings<K, V>,
  private val consumer: KafkaConsumer<K, V>,
  isRetriableException: (Throwable) -> Boolean,
  ackMode: AckMode,
) {
  val awaitingTransaction = AtomicBoolean()
  private val atmostOnceOffsets: AtmostOnceOffsets = AtmostOnceOffsets()
  private val eventScheduler: ExecutorCoroutineDispatcher = kafkaConsumerDispatcher(receiverOptions.groupId)
  
  // KafkaScheduler.newEvent(receiverOptions.groupId)
  val collector = Channel<ConsumerRecord<K, V>>()
  private val consumerEventLoop: ConsumerEventLoop<K, V> = ConsumerEventLoop(
    ackMode,
    atmostOnceOffsets,
    receiverOptions,
    eventScheduler,
    consumer,
    isRetriableException,
    collector,
    awaitingTransaction
  )
  
  fun receive(): Flow<ConsumerRecord<K, V>> {
    return collector.consumeAsFlow()
  }
  
  suspend fun close(): Unit = try {
    consumerEventLoop.stop()
  } finally {
    eventScheduler.close()
  }
  
  suspend fun commit(record: ConsumerRecord<K, V>): Unit {
    val offset = record.offset()
    val partition = TopicPartition(record.topic(), record.partition())
    val committedOffset: Long = atmostOnceOffsets.committedOffset(partition)
    atmostOnceOffsets.onDispatch(partition, offset)
    // val commitAheadSize: Long = receiverOptions.atmostOnceCommitAheadSize().toLong()
    val commitAheadSize: Long = 5
    val committable: ReceiverOffset = CommittableOffset<K, V>(
      partition,
      offset + commitAheadSize,
      consumerEventLoop.commitEvent,
      // receiverOptions.commitBatchSize()
      5
    )
    if (offset >= committedOffset) {
      return committable.commit()
    } else if (committedOffset - offset >= commitAheadSize / 2) {
      // TODO this should be launched
      // committable.commit().subscribe()
    }
    return Unit
  }
  
  fun acknowledge(record: ConsumerRecord<K, V>) {
    toCommittableOffset(record).acknowledge()
  }
  
  private fun toCommittableOffset(record: ConsumerRecord<K, V>): CommittableOffset<K, V> =
    CommittableOffset(
      TopicPartition(record.topic(), record.partition()),
      record.offset(),
      consumerEventLoop.commitEvent,
      // receiverOptions.commitBatchSize()
      5 //  TODO batch size in settings
    )
  
  private class CommittableOffset<K, V>(
    private val topicPartition: TopicPartition,
    private val commitOffset: Long,
    commitEvent: ConsumerEventLoop<K, V>.CommitEvent,
    commitBatchSize: Int,
  ) : ReceiverOffset {
    private val commitEvent: ConsumerEventLoop<K, V>.CommitEvent
    private val commitBatchSize: Int
    private val acknowledged = AtomicBoolean(false)
    
    init {
      this.commitEvent = commitEvent
      this.commitBatchSize = commitBatchSize
    }
    
    public override suspend fun commit(): Unit =
      if (maybeUpdateOffset() > 0) scheduleCommit() else Unit
    
    public override fun acknowledge() {
      val uncommittedCount = maybeUpdateOffset().toLong()
      if (commitBatchSize in 1..uncommittedCount) commitEvent.scheduleIfRequired()
    }
    
    public override fun topicPartition(): TopicPartition {
      return topicPartition
    }
    
    public override fun offset(): Long {
      return commitOffset
    }
    
    private fun maybeUpdateOffset(): Int {
      return if (acknowledged.compareAndSet(false, true)) commitEvent.commitBatch.updateOffset(
        topicPartition,
        commitOffset
      ) else commitEvent.commitBatch.batchSize()
    }
    
    // private fun scheduleCommit(): Mono<Void> {
    //   return Mono.create<Void>(java.util.function.Consumer<MonoSink<Void?>> { emitter: MonoSink<Void?>? ->
    //     commitEvent.commitBatch.addCallbackEmitter(emitter)
    //     commitEvent.scheduleIfRequired()
    //   })
    // }
    
    private suspend fun scheduleCommit(): Unit {
    }
    
    override fun toString(): String =
      "$topicPartition@$commitOffset"
  }
  
  companion object {
    /** Note: Methods added to this set should also be included in javadoc for [KafkaReceiver.doOnConsumer]  */
    private val DELEGATE_METHODS: Set<String> = HashSet(
      Arrays.asList(
        "assignment",
        "subscription",
        "seek",
        "seekToBeginning",
        "seekToEnd",
        "position",
        "committed",
        "metrics",
        "partitionsFor",
        "listTopics",
        "paused",
        "pause",
        "resume",
        "offsetsForTimes",
        "beginningOffsets",
        "endOffsets"
      )
    )
    
    private fun <K, V, T> withHandler(
      receiverOptions: ConsumerSettings<K, V>,
      ackMode: AckMode,
      function: (ExecutorCoroutineDispatcher, ConsumerHandler<K, V>) -> Flow<T>,
    ): Flow<T> = flow {
      KafkaConsumer(receiverOptions).use { consumer ->
        ConsumerHandler<K, V>(
          receiverOptions,
          consumer,
          // Always use the currently set value
          { e -> e is RetriableCommitFailedException },
          ackMode
        )
      }
    }
  }
}
