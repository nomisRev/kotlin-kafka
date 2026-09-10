package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.Offset
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.github.nomisRev.kafka.receiver.internals.EventLoop
import java.time.Duration as JavaDuration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
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
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test

/**
 * When the collector cannot keep up, [EventLoop] hands the records over from a coroutine it
 * launches in the collector's context. These tests pin down that this coroutine belongs to the
 * receiver and not to the collector: its failures have to reach the collector *through the
 * receive flow*, and shutting the receiver down has to take it along.
 */
class BackPressureContainmentSpec {

  @Test
  fun `a commit failure before the queued send runs fails the receive flow`() = runBlocking {
    val commitFailure = KafkaException("commit raced a group rebalance")
    val heldCommit = CompletableDeferred<HeldCommit>()
    val consumer = holdingTheFirstCommit(heldCommit).seededWith(recordCount = 2)

    val receiver = dispatcherScope(RECEIVER_THREAD)
    val collector = dispatcherScope(COLLECTOR_THREAD)
    val firstOffset = CompletableDeferred<Offset>()
    val releaseCollector = CountDownLatch(1)
    val flowFailure = CompletableDeferred<Throwable>()
    val batches = AtomicInteger(0)

    val collecting = collector.scope.launch {
      val loop = EventLoop(
        topicNames = setOf(BACK_PRESSURE_TOPIC),
        settings = backPressureSettings(),
        consumer = consumer,
        scope = receiver.scope,
        outerContext = currentCoroutineContext(),
      )
      loop.receive()
        .catch { e -> flowFailure.complete(e) }
        .collect { records ->
          if (batches.incrementAndGet() == 1) {
            firstOffset.complete(loop.offsetFromRecord(records.first()))
            /* Block the collector's *thread*, not just its coroutine: the send the event loop
             * launches onto this dispatcher then stays queued and has not reached channel.send
             * by the time the commit below closes the channel. That is the window the incident
             * in #217 fell into - the send enters a channel that is already closed. */
            releaseCollector.await()
          }
        }
    }
    /* Only survives if the failing send stays out of the collector's scope. */
    val bystander = collector.scope.launch { delay(60.seconds) }

    try {
      withTimeout(30.seconds) { consumer.secondBatchPolled.await() }
      /* Acknowledging from here, not from the collector, whose thread is blocked. */
      withTimeout(30.seconds) { firstOffset.await() }.acknowledge()
      withTimeout(30.seconds) { consumer.backPressured.await() }

      val commit = withTimeout(30.seconds) { heldCommit.await() }
      withContext(receiver.dispatcher) { commit.failWith(commitFailure) }
      releaseCollector.countDown()

      val failure = withTimeout(30.seconds) { flowFailure.await() }
      /* kotlinx.coroutines copies an exception when it crosses coroutines to recover its stack
       * trace, so the collector sees an equal failure, not the very same instance. */
      assertEquals(commitFailure::class, failure::class)
      assertEquals(commitFailure.message, failure.message)

      assertTrue(bystander.isActive, "the collector's scope must survive the failing send")
      assertEquals(emptyList<Throwable>(), collector.uncaught.toList())
      assertEquals(emptyList<Throwable>(), receiver.uncaught.toList())
    } finally {
      releaseCollector.countDown()
      bystander.cancel()
      collecting.cancelAndJoin()
      collector.close()
      receiver.close()
    }
  }

  @Test
  fun `a commit failure while the send is waiting still delivers its batch`() = runBlocking {
    val commitFailure = KafkaException("commit raced a group rebalance")
    val heldCommit = CompletableDeferred<HeldCommit>()
    val consumer = holdingTheFirstCommit(heldCommit).seededWith(recordCount = 2)
    val collecting = startBackPressuredCollecting(consumer)
    val bystander = collecting.collector.scope.launch { delay(60.seconds) }

    try {
      withTimeout(30.seconds) { collecting.backPressureTheSecondBatch() }
      val commit = withTimeout(30.seconds) { heldCommit.await() }
      withContext(collecting.receiver.dispatcher) { commit.failWith(commitFailure) }
      collecting.resumeCollecting.complete(Unit)

      /* `close` is graceful towards a sender that is already suspended in `send`: its batch is
       * still handed over, and only the receive after it carries the cause. Both have to reach
       * the collector through the flow. */
      withTimeout(30.seconds) { collecting.secondBatchReceived.await() }
      val failure = withTimeout(30.seconds) { collecting.flowFailure.await() }
      assertEquals(commitFailure::class, failure::class)
      assertEquals(commitFailure.message, failure.message)

      assertTrue(bystander.isActive, "the collector's scope must survive the closed channel")
    } finally {
      bystander.cancel()
      collecting.close()
    }
  }

  @Test
  fun `a send that fails on its own closes the channel instead of cancelling the collector's scope`() = runBlocking {
    val wakeupFailure = IllegalStateException("consumer failed while resuming from back pressure")
    val failNextWakeup = AtomicBoolean(false)
    val sendThread = CompletableDeferred<String>()

    val consumer = object : BackPressuredConsumer() {
      /* `wakeup` is the only consumer call the send makes after handing its records over, which
       * makes it the place to inject a failure into that very coroutine. It is called exactly
       * once from there, and the event loop itself does not wake a consumer it just paused. */
      override fun wakeup() {
        if (failNextWakeup.compareAndSet(true, false)) {
          sendThread.complete(Thread.currentThread().name)
          throw wakeupFailure
        } else super.wakeup()
      }
    }.seededWith(recordCount = 2)

    val collecting = startBackPressuredCollecting(consumer)
    val bystander = collecting.collector.scope.launch { delay(60.seconds) }
    try {
      withTimeout(30.seconds) { collecting.backPressureTheSecondBatch() }

      failNextWakeup.set(true)
      collecting.resumeCollecting.complete(Unit)

      val failure = withTimeout(30.seconds) { collecting.flowFailure.await() }
      assertEquals(wakeupFailure::class, failure::class)
      assertEquals(wakeupFailure.message, failure.message)

      val thread = withTimeout(30.seconds) { sendThread.await() }
      assertTrue(
        thread.startsWith(COLLECTOR_THREAD),
        "the send keeps the collector's dispatcher and only drops its job, but ran on $thread"
      )
      assertTrue(bystander.isActive, "the failing send must leave the collector's scope alone")
    } finally {
      bystander.cancel()
      collecting.close()
    }
  }

  @Test
  fun `the pending send belongs to the receiver, not to the collector`() = runBlocking {
    val collecting = startBackPressuredCollecting(BackPressuredConsumer().seededWith(recordCount = 2))
    try {
      withTimeout(30.seconds) { collecting.backPressureTheSecondBatch() }

      /* Shutting the receiver down has to take the pending send with it. Owned by the collector
       * it would survive its own receiver and keep handing out records that belong to a consumer
       * which is already being closed. */
      collecting.receiver.close()
      collecting.resumeCollecting.complete(Unit)

      assertNull(
        withTimeoutOrNull(2.seconds) { collecting.secondBatchReceived.await() },
        "a closed receiver must not hand any more records to the collector"
      )
      /* Cancelling the send is not a failure of it: a send that mistook its own cancellation for
       * one would close the channel with it, and the collector would see a receive flow that
       * failed rather than one that simply ended. */
      assertNull(
        collecting.flowFailure.takeIf { it.isCompleted }?.await(),
        "closing the receiver must not surface as a failure of the receive flow"
      )
    } finally {
      collecting.close()
    }
  }

  @Test
  fun `cancelling the collector while a send is pending is not a failure`() = runBlocking {
    val collecting = startBackPressuredCollecting(BackPressuredConsumer().seededWith(recordCount = 2))
    try {
      withTimeout(30.seconds) {
        collecting.backPressureTheSecondBatch()
        collecting.job.cancelAndJoin()
      }
      collecting.receiver.close()

      assertEquals(emptyList<Throwable>(), collecting.receiver.uncaught.toList())
      assertEquals(emptyList<Throwable>(), collecting.collector.uncaught.toList())
    } finally {
      collecting.close()
    }
  }

  @Test
  fun `back pressure delivers every record in order once the collector catches up`() = runBlocking {
    val recordCount = 10
    val consumer = BackPressuredConsumer().seededWith(recordCount)
    val receiver = dispatcherScope(RECEIVER_THREAD)
    val collector = dispatcherScope(COLLECTOR_THREAD)
    val collected = CopyOnWriteArrayList<String>()
    val flowFailure = CompletableDeferred<Throwable>()

    val collecting = collector.scope.launch {
      val loop = EventLoop(
        topicNames = setOf(BACK_PRESSURE_TOPIC),
        settings = backPressureSettings(),
        consumer = consumer,
        scope = receiver.scope,
        outerContext = currentCoroutineContext(),
      )
      loop.receive()
        .catch { e -> flowFailure.complete(e) }
        .collect { records ->
          /* Slow enough that the event loop runs into back pressure repeatedly - the consumer is
           * paused meanwhile, so the batches it hands over afterwards vary in size. */
          delay(20)
          records.forEach { record ->
            collected += record.value()
            loop.offsetFromRecord(record).acknowledge()
          }
        }
    }

    try {
      withTimeout(30.seconds) {
        while (collected.size < recordCount && !flowFailure.isCompleted) delay(20)
      }
      assertNull(flowFailure.takeIf { it.isCompleted }?.await(), "the receive flow must not fail")
      assertEquals((0 until recordCount).map { "value-$it" }, collected.toList())
      assertTrue(consumer.backPressured.isCompleted, "this test only means something under back pressure")
    } finally {
      collecting.cancelAndJoin()
      collector.close()
      receiver.close()
    }
  }
}

private const val BACK_PRESSURE_TOPIC = "back-pressure-topic"
private const val COLLECTOR_THREAD = "back-pressure-collector"

/* The event loop asserts that it runs on a thread named like the library's own dispatcher. */
private const val RECEIVER_THREAD = "kotlin-kafka-back-pressure-group"

private val BACK_PRESSURE_PARTITION = TopicPartition(BACK_PRESSURE_TOPIC, 0)

private fun backPressureSettings(): ReceiverSettings<String, String> =
  ReceiverSettings(
    bootstrapServers = "unused:9092",
    keyDeserializer = StringDeserializer(),
    valueDeserializer = StringDeserializer(),
    groupId = "back-pressure-group",
    commitStrategy = CommitStrategy.BySize(1),
    closeTimeout = 5.seconds,
  )

/** A [MockConsumer] that reports the two moments the tests need to synchronise on. */
private open class BackPressuredConsumer : MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
  private val nonEmptyPolls = AtomicInteger(0)

  /** A batch was polled that the collector is not waiting for, so its `trySend` fails. */
  val secondBatchPolled = CompletableDeferred<Unit>()

  /**
   * The event loop paused the consumer. It only does so on a poll it runs while back pressured,
   * so from here on the second batch is known to be inside the send.
   */
  val backPressured = CompletableDeferred<Unit>()

  override fun poll(timeout: JavaDuration): ConsumerRecords<String, String> =
    super.poll(timeout).also {
      if (!it.isEmpty && nonEmptyPolls.incrementAndGet() == 2) secondBatchPolled.complete(Unit)
    }

  override fun pause(partitions: MutableCollection<TopicPartition>) {
    super.pause(partitions)
    backPressured.complete(Unit)
  }
}

/** Keeps the first commit open in [heldCommit], so a test can fail it at a moment of its choosing. */
private fun holdingTheFirstCommit(heldCommit: CompletableDeferred<HeldCommit>): BackPressuredConsumer =
  object : BackPressuredConsumer() {
    override fun commitAsync(
      offsets: MutableMap<TopicPartition, OffsetAndMetadata>,
      callback: OffsetCommitCallback?,
    ) {
      requireNotNull(callback) { "the event loop always commits with a callback" }
      if (!heldCommit.complete(HeldCommit(offsets, callback))) callback.onComplete(offsets, null)
    }
  }

private class HeldCommit(
  private val offsets: MutableMap<TopicPartition, OffsetAndMetadata>,
  private val callback: OffsetCommitCallback,
) {
  /* Has to run on the consumer thread: the event loop asserts that in its commit handling. */
  fun failWith(error: Exception): Unit = callback.onComplete(offsets, error)
}

private fun <A : MockConsumer<String, String>> A.seededWith(recordCount: Int): A = apply {
  updateBeginningOffsets(mapOf(BACK_PRESSURE_PARTITION to 0L))
  repeat(recordCount) { offset ->
    schedulePollTask {
      if (offset == 0) rebalance(listOf(BACK_PRESSURE_PARTITION))
      addRecord(
        ConsumerRecord(
          BACK_PRESSURE_TOPIC,
          BACK_PRESSURE_PARTITION.partition(),
          offset.toLong(),
          "key-$offset",
          "value-$offset",
        )
      )
    }
  }
}

private class BackPressureCollecting(
  val consumer: BackPressuredConsumer,
  val receiver: DispatcherScope,
  val collector: DispatcherScope,
  val job: Job,
  val flowFailure: CompletableDeferred<Throwable>,
  val secondBatchReceived: CompletableDeferred<Unit>,
  val acknowledgeFirstBatch: CompletableDeferred<Unit>,
  val resumeCollecting: CompletableDeferred<Unit>,
) {
  suspend fun close() {
    job.cancelAndJoin()
    collector.close()
    receiver.close()
  }

  /**
   * Drives the event loop into back pressure, so that the second batch sits in a send which is
   * suspended in `channel.send`.
   *
   * Acknowledging is what triggers the commit, and the poll the event loop runs right after
   * requesting that commit is what pauses the consumer. Acknowledging only *after* the second
   * batch was polled keeps that order: the acknowledgement reaches the consumer thread as a new
   * task, which cannot run before the poll that decided to back pressure has finished.
   */
  suspend fun backPressureTheSecondBatch() {
    consumer.secondBatchPolled.await()
    acknowledgeFirstBatch.complete(Unit)
    consumer.backPressured.await()
  }
}

/**
 * Collects an [EventLoop] on [consumer] in a way that forces back pressure: the collector holds on
 * to the first batch, so every batch after it can only reach the collector through the send.
 */
private fun startBackPressuredCollecting(consumer: BackPressuredConsumer): BackPressureCollecting {
  val receiver = dispatcherScope(RECEIVER_THREAD)
  val collector = dispatcherScope(COLLECTOR_THREAD)
  val flowFailure = CompletableDeferred<Throwable>()
  val secondBatchReceived = CompletableDeferred<Unit>()
  val acknowledgeFirstBatch = CompletableDeferred<Unit>()
  val resumeCollecting = CompletableDeferred<Unit>()
  val batches = AtomicInteger(0)

  val job = collector.scope.launch {
    val loop = EventLoop(
      topicNames = setOf(BACK_PRESSURE_TOPIC),
      settings = backPressureSettings(),
      consumer = consumer,
      scope = receiver.scope,
      outerContext = currentCoroutineContext(),
    )
    loop.receive()
      .catch { e -> flowFailure.complete(e) }
      .collect { records ->
        if (batches.incrementAndGet() == 1) {
          acknowledgeFirstBatch.await()
          records.forEach { record -> loop.offsetFromRecord(record).acknowledge() }
          resumeCollecting.await()
        } else secondBatchReceived.complete(Unit)
      }
  }

  return BackPressureCollecting(
    consumer = consumer,
    receiver = receiver,
    collector = collector,
    job = job,
    flowFailure = flowFailure,
    secondBatchReceived = secondBatchReceived,
    acknowledgeFirstBatch = acknowledgeFirstBatch,
    resumeCollecting = resumeCollecting,
  )
}

private class DispatcherScope(
  val scope: CoroutineScope,
  val dispatcher: ExecutorCoroutineDispatcher,
  val uncaught: CopyOnWriteArrayList<Throwable>,
) {
  suspend fun close() {
    scope.coroutineContext[Job]?.cancelAndJoin()
    dispatcher.close()
  }
}

private fun dispatcherScope(threadName: String): DispatcherScope {
  val dispatcher = Executors.newSingleThreadExecutor { runnable -> Thread(runnable, threadName) }
    .asCoroutineDispatcher()
  val uncaught = CopyOnWriteArrayList<Throwable>()
  val handler = CoroutineExceptionHandler { _, throwable -> uncaught += throwable }
  return DispatcherScope(CoroutineScope(Job() + dispatcher + handler), dispatcher, uncaught)
}
