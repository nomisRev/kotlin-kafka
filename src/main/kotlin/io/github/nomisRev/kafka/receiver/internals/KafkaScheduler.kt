package io.github.nomisRev.kafka.receiver.internals

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * [org.apache.kafka.clients.consumer.KafkaConsumer] is single-threaded,
 * and thus needs to have a dedicated [ExecutorCoroutineDispatcher] that guarantees a single thread
 * where we can schedule our interactions through [org.apache.kafka.clients.consumer.KafkaConsumer].
 *
 * This [Flow] returns a scheduler and CoroutineScope that is scoped to the stream,
 * it gets lazily initialized when the [Flow] is collected and gets closed when the flow terminates.
 */
internal fun kafkaScheduler(groupId: String): Flow<Pair<CoroutineScope, ExecutorCoroutineDispatcher>> = flow {
  kafkaConsumerDispatcher(groupId).use { dispatcher: ExecutorCoroutineDispatcher ->
    val job = Job()
    val scope = CoroutineScope(job + dispatcher + defaultCoroutineExceptionHandler)
    try {
      emit(Pair(scope, dispatcher))
    } finally {
      job.cancelAndJoin()
    }
  }
}

// All exceptions inside the library code should be handled.
// So any uncaught errors on the KafkaConsumer dispatcher is a bug.
private val defaultCoroutineExceptionHandler = CoroutineExceptionHandler { coroutineContext, throwable ->
  log.error(
    "KafkaDispatcher with $coroutineContext failed with an uncaught exception. Report to kotlin-kafka repo please.",
    throwable
  )
}

private fun kafkaConsumerDispatcher(groupId: String): ExecutorCoroutineDispatcher =
  ScheduledThreadPoolExecutor(1, EventThreadFactory(groupId)).apply {
    removeOnCancelPolicy = true
    maximumPoolSize = 1
  }.asCoroutineDispatcher()

private const val PREFIX = "kotlin-kafka-"
private val COUNTER_REFERENCE = AtomicLong()

// Custom [ThreadFactory] to give a more meaningful name: "kotlin-kafka-groupId-counter"
private class EventThreadFactory(private val groupId: String) : ThreadFactory {
  override fun newThread(runnable: Runnable): Thread =
    Thread(runnable, "$PREFIX$groupId-${COUNTER_REFERENCE.incrementAndGet()}")
}
