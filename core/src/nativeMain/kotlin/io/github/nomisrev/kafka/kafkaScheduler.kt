package io.github.nomisrev.kafka

import kotlin.native.concurrent.AtomicLong
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.newSingleThreadContext

internal actual fun kafkaScheduler(groupId: String): Flow<Pair<CoroutineScope, CoroutineDispatcher>> {
  val name = "$PREFIX$groupId-${COUNTER_REFERENCE.addAndGet(1)}"
  val dispatcher = newSingleThreadContext(name)
  val job = Job()
  val scope = CoroutineScope(job + dispatcher + defaultCoroutineExceptionHandler)
  return flow {
    try {
      emit(Pair(scope, dispatcher))
    } finally {
      job.cancelAndJoin()
      dispatcher.close()
    }
  }
}

private val defaultCoroutineExceptionHandler = CoroutineExceptionHandler { coroutineContext, throwable ->
  println("KafkaDispatcher with $coroutineContext failed with an uncaught exception. Report to kotlin-kafka repo please.")
  throwable.printStackTrace()
}

private const val PREFIX = "kotlin-kafka-"
private val COUNTER_REFERENCE = AtomicLong()
