package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.github.nomisRev.kafka.receiver.internals.EventLoop
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test

/**
 * Partitions that were *lost* - the member already left the group - must not be committed. The broker answers such a
 * commit with `CommitFailedException`, which is not retryable and would end the receive flow over something the
 * consumer recovers from by rejoining.
 */
class PartitionsLostSpec {

  @Test
  fun `losing partitions does not commit them`() = runBlocking {
    val consumer = ListenerCapturingConsumer().seededWithOneRecord()
    val collecting = collectAcknowledging(consumer)

    try {
      withTimeout(30.seconds) { consumer.firstRecordAcknowledged.await() }

      consumer.callListenerOnConsumerThread { it.onPartitionsLost(mutableListOf(LOST_PARTITION)) }

      assertEquals(emptyList(), consumer.committedOffsets.toList(), "lost partitions must not be committed")
      assertNull(
        collecting.flowFailure.takeIf { it.isCompleted }?.await(),
        "losing partitions must not end the receive flow"
      )
    } finally {
      collecting.close()
    }
  }

  @Test
  fun `losing partitions does not commit them on a later tick either`() = runBlocking {
    val consumer = ListenerCapturingConsumer().seededWithOneRecord()
    val collecting = collectAcknowledging(consumer)

    try {
      withTimeout(30.seconds) { consumer.firstRecordAcknowledged.await() }

      consumer.callListenerOnConsumerThread { it.onPartitionsLost(mutableListOf(LOST_PARTITION)) }

      /* Not committing *during* the callback is only half of it: the acknowledged offsets stay in the batch
       * until they are dropped, and the periodic commit of `ByTime` is what would send them afterwards. */
      delay(COMMIT_INTERVAL * 3)

      assertEquals(
        emptyList(),
        consumer.committedOffsets.toList(),
        "a commit after the partitions were lost must not carry them either"
      )
      assertNull(
        collecting.flowFailure.takeIf { it.isCompleted }?.await(),
        "losing partitions must not end the receive flow"
      )
    } finally {
      collecting.close()
    }
  }

  @Test
  fun `revoking partitions still commits them`() = runBlocking {
    val consumer = ListenerCapturingConsumer().seededWithOneRecord()
    val collecting = collectAcknowledging(consumer)

    try {
      withTimeout(30.seconds) { consumer.firstRecordAcknowledged.await() }

      consumer.callListenerOnConsumerThread { it.onPartitionsRevoked(mutableListOf(LOST_PARTITION)) }

      withTimeout(30.seconds) {
        while (consumer.committedOffsets.isEmpty()) delay(20)
      }
      assertEquals(1L, consumer.committedOffsets.first()[LOST_PARTITION]?.offset())
    } finally {
      collecting.close()
    }
  }
}

private const val LOST_TOPIC = "partitions-lost-topic"
private val COMMIT_INTERVAL = 1.seconds
private val LOST_PARTITION = TopicPartition(LOST_TOPIC, 0)
private const val LOST_RECEIVER_THREAD = "kotlin-kafka-partitions-lost-group"

private fun lostSettings(): ReceiverSettings<String, String> =
  ReceiverSettings(
    bootstrapServers = "unused:9092",
    keyDeserializer = StringDeserializer(),
    valueDeserializer = StringDeserializer(),
    groupId = "partitions-lost-group",
    commitStrategy = CommitStrategy.ByTime(COMMIT_INTERVAL),
    closeTimeout = 5.seconds,
  )

private open class ListenerCapturingConsumer : MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
  private val listener = CompletableDeferred<ConsumerRebalanceListener>()
  private val callListener = CompletableDeferred<(ConsumerRebalanceListener) -> Unit>()
  private val invoking = AtomicBoolean(false)
  private val listenerReturned = CompletableDeferred<Unit>()

  val committedOffsets = CopyOnWriteArrayList<Map<TopicPartition, OffsetAndMetadata>>()
  val firstRecordAcknowledged = CompletableDeferred<Unit>()

  override fun subscribe(topics: MutableCollection<String>, callback: ConsumerRebalanceListener) {
    listener.complete(callback)
    super.subscribe(topics, callback)
  }

  override fun commitAsync(
    offsets: MutableMap<TopicPartition, OffsetAndMetadata>,
    callback: OffsetCommitCallback?,
  ) {
    requireNotNull(callback) { "the event loop always commits with a callback" }
    committedOffsets += offsets.toMap()
    callback.onComplete(offsets, null)
  }

  /* The event loop asserts it runs on its own thread, so the callback is invoked from inside a poll. */
  override fun poll(timeout: java.time.Duration): org.apache.kafka.clients.consumer.ConsumerRecords<String, String> {
    if (callListener.isCompleted && invoking.compareAndSet(false, true)) {
      @Suppress("OPT_IN_USAGE")
      try {
        callListener.getCompleted()(listener.getCompleted())
      } finally {
        // only now has the callback run to completion, so only now may the test assert on its effects
        listenerReturned.complete(Unit)
      }
    }
    return super.poll(timeout)
  }

  suspend fun callListenerOnConsumerThread(action: (ConsumerRebalanceListener) -> Unit) {
    listener.await()
    callListener.complete(action)
    withTimeout(10.seconds) { listenerReturned.await() }
  }
}

private fun <A : MockConsumer<String, String>> A.seededWithOneRecord(): A = apply {
  updateBeginningOffsets(mapOf(LOST_PARTITION to 0L))
  schedulePollTask {
    rebalance(listOf(LOST_PARTITION))
    addRecord(ConsumerRecord(LOST_TOPIC, 0, 0L, "key", "value"))
  }
}

private class LostCollecting(
  private val job: Job,
  private val scope: CoroutineScope,
  private val dispatcher: ExecutorCoroutineDispatcher,
  val flowFailure: CompletableDeferred<Throwable>,
) {
  suspend fun close() {
    job.cancelAndJoin()
    scope.coroutineContext[Job]?.cancelAndJoin()
    dispatcher.close()
  }
}

private fun collectAcknowledging(consumer: ListenerCapturingConsumer): LostCollecting {
  val dispatcher =
    Executors.newSingleThreadExecutor { runnable -> Thread(runnable, LOST_RECEIVER_THREAD) }.asCoroutineDispatcher()
  val scope = CoroutineScope(Job() + dispatcher)
  val flowFailure = CompletableDeferred<Throwable>()

  val job = scope.launch {
    val loop = EventLoop(
      topicNames = setOf(LOST_TOPIC),
      settings = lostSettings(),
      consumer = consumer,
      scope = scope,
      outerContext = currentCoroutineContext(),
    )
    loop.receive()
      .catch { e -> flowFailure.complete(e) }
      .collect { records ->
        records.forEach { record -> loop.offsetFromRecord(record).acknowledge() }
        consumer.firstRecordAcknowledged.complete(Unit)
      }
  }

  return LostCollecting(job, scope, dispatcher, flowFailure)
}
