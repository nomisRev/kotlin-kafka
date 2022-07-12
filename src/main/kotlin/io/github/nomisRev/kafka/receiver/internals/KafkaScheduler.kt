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

private const val PREFIX = "kotlin-kafka-"
private val COUNTER_REFERENCE = AtomicLong()

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

/*
 * Create a single threaded [KafkaConsumer] dispatcher.
 * We'll use this dispatcher to schedule all KafkaConsumer interactions to guarantee single thread usage.
 */
private fun kafkaConsumerDispatcher(groupId: String): ExecutorCoroutineDispatcher =
  (Executors.newScheduledThreadPool(1, EventThreadFactory(groupId)) as ScheduledThreadPoolExecutor).apply {
    removeOnCancelPolicy = true
    maximumPoolSize = 1
  }.asCoroutineDispatcher()

private class EventThreadFactory(private val groupId: String) : ThreadFactory {
  override fun newThread(runnable: Runnable): Thread =
    Thread(runnable, "$PREFIX$groupId-${COUNTER_REFERENCE.incrementAndGet()}")
}