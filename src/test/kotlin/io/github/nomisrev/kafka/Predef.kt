package io.github.nomisrev.kafka

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.map
import kotlin.test.assertTrue

inline fun <A, B> Flow<A>.mapIndexed(
  crossinline transform: suspend (index: Int, value: A) -> B,
): Flow<B> {
  var index = 0
  return map { value ->
    transform(index++, value)
  }
}

inline fun <reified A : Throwable> assertThrows(
  message: String? = "Expected exception ${A::class.java}, but code didn't throw any exception.",
  block: () -> Unit,
): A {
  val exception = try {
    block()
    null
  } catch (e: Throwable) {
    e
  }
    ?: throw AssertionError(message)
  assertTrue(exception is A, "Expected exception of ${A::class.java} but found ${exception.javaClass.name}")
  return exception
}

suspend fun <A> FlowCollector<A>.emitAll(iterable: Iterable<A>): Unit =
  iterable.forEach { emit(it) }