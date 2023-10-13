package io.github.nomisrev.kafka

import io.kotest.assertions.fail
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

inline fun <A, B> Flow<A>.mapIndexed(
  crossinline transform: suspend (index: Int, value: A) -> B,
): Flow<B> {
  var index = 0
  return map { value ->
    transform(index++, value)
  }
}

suspend fun CoroutineScope.shouldCancel(block: suspend () -> Unit): CancellationException {
  val cancel = CompletableDeferred<CancellationException>()
  try {
    block()
    fail("Expected to be cancellable, but wasn't")
  } catch (e: CancellationException) {
    cancel.complete(e)
  }
  return cancel.await()
}