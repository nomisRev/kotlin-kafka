package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.github.nomisRev.kafka.receiver.internals.EventLoop
import java.time.Duration as JavaDuration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test

/**
 * The event loop remembers the partitions the user had paused when it starts back pressuring, so
 * that it can leave them paused after a rebalance. A rebalance that takes those partitions away
 * has to survive that bookkeeping.
 */
class RebalanceDuringBackPressureSpec {

  @Test
  fun `a rebalance that drops user paused partitions does not break the subscription`() = runBlocking {
    val rebalanceNow = AtomicBoolean(false)
    val backPressured = CompletableDeferred<Unit>()
    val secondBatchPolled = CompletableDeferred<Unit>()
    val nonEmptyPolls = AtomicInteger(0)
    val listener = CompletableDeferred<ConsumerRebalanceListener>()
    val rebalanceHappened = CompletableDeferred<Unit>()
    val releaseCollector = CompletableDeferred<Unit>()

    val consumer = object : MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
      override fun subscribe(topics: MutableCollection<String>, callback: ConsumerRebalanceListener) {
        listener.complete(callback)
        super.subscribe(topics, callback)
      }

      /* A real consumer runs the rebalance callbacks from inside poll; MockConsumer does not
       * (it has a "TODO: Rebalance callbacks"), so the probe does it here instead. */
      override fun poll(timeout: JavaDuration): ConsumerRecords<String, String> {
        if (rebalanceNow.compareAndSet(true, false)) {
          try {
            listener.getCompleted().onPartitionsAssigned(mutableListOf(REPAUSE_PARTITIONS[0]))
          } finally {
            rebalanceHappened.complete(Unit)
          }
        }
        return super.poll(timeout).also {
          if (!it.isEmpty && nonEmptyPolls.incrementAndGet() == 2) secondBatchPolled.complete(Unit)
        }
      }

      override fun pause(partitions: MutableCollection<TopicPartition>) {
        super.pause(partitions)
        /* Only the loop's own "pause everything" completes this, not the probe's user pause. */
        if (partitions.size == REPAUSE_PARTITIONS.size) backPressured.complete(Unit)
      }
    }

    consumer.updateBeginningOffsets(REPAUSE_PARTITIONS.associateWith { 0L })
    var record = 0L
    repeat(2) {
      consumer.schedulePollTask {
        if (record == 0L) {
          consumer.rebalance(REPAUSE_PARTITIONS)
          /* Two partitions the user paused: the loop remembers them when it back pressures. */
          consumer.pause(REPAUSE_PARTITIONS.drop(1).toMutableList())
        }
        consumer.addRecord(ConsumerRecord(REPAUSE_TOPIC, 0, record, "key-$record", "value-$record"))
        record++
      }
    }

    val dispatcher = Executors.newSingleThreadExecutor { r -> Thread(r, "kotlin-kafka-probe-group") }
      .asCoroutineDispatcher()
    val scope = CoroutineScope(Job() + dispatcher)
    val collectorDispatcher = Executors.newSingleThreadExecutor { r -> Thread(r, "probe-collector") }
      .asCoroutineDispatcher()
    val collectorScope = CoroutineScope(Job() + collectorDispatcher)
    val flowFailure = CompletableDeferred<Throwable>()
    val acknowledge = CompletableDeferred<Unit>()
    val batches = AtomicInteger(0)

    val collecting = collectorScope.launch {
      val loop = EventLoop(
        topicNames = setOf(REPAUSE_TOPIC),
        settings = repauseSettings(),
        consumer = consumer,
        scope = scope,
        outerContext = currentCoroutineContext(),
      )
      loop.receive()
        .catch { e -> flowFailure.complete(e) }
        .collect { records ->
          if (batches.incrementAndGet() == 1) {
            acknowledge.await()
            records.forEach { r -> loop.offsetFromRecord(r).acknowledge() }
            releaseCollector.await()
          }
        }
    }

    try {
      withTimeout(30.seconds) { secondBatchPolled.await() }
      acknowledge.complete(Unit)
      withTimeout(30.seconds) { backPressured.await() }

      rebalanceNow.set(true)
      withTimeout(30.seconds) { rebalanceHappened.await() }
      releaseCollector.complete(Unit)
      val failure = withTimeoutOrNull(10.seconds) { flowFailure.await() }

      assertNull(
        failure,
        "the rebalance must not fail the receive flow, but it failed with " +
          "${failure?.let { "${it::class.simpleName}: ${it.message}" }}"
      )
    } finally {
      collecting.cancelAndJoin()
      collectorScope.coroutineContext[Job]?.cancelAndJoin()
      scope.coroutineContext[Job]?.cancelAndJoin()
      dispatcher.close()
      collectorDispatcher.close()
    }
  }
}

private const val REPAUSE_TOPIC = "probe-topic"
private val REPAUSE_PARTITIONS = listOf(0, 1, 2).map { TopicPartition(REPAUSE_TOPIC, it) }

private fun repauseSettings(): ReceiverSettings<String, String> =
  ReceiverSettings(
    bootstrapServers = "unused:9092",
    keyDeserializer = StringDeserializer(),
    valueDeserializer = StringDeserializer(),
    groupId = "probe-group",
    commitStrategy = CommitStrategy.BySize(1),
    closeTimeout = 5.seconds,
  )
