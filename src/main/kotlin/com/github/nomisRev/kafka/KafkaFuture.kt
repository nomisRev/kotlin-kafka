package com.github.nomisRev.kafka

import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutionException
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.cancelFutureOnCompletion
import kotlinx.coroutines.handleCoroutineException
import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.clients.admin.DeleteTopicsResult
import org.apache.kafka.common.KafkaFuture

/** Await all [DeleteTopicsResult] in a suspending way. */
public suspend fun DeleteTopicsResult.await() {
  all().await()
}

/** Await all [CreateTopicsResult] in a suspending way. */
public suspend fun CreateTopicsResult.await() {
  all().await()
}

/**
 * Await a [KafkaFuture] in a suspending way. Code inspired by
 * [KotlinX Coroutines JDK8](https://github.com/Kotlin/kotlinx.coroutines/tree/master/integration/kotlinx-coroutines-jdk8)
 */
public suspend fun <T> KafkaFuture<T>.await(): T {
  // fast path when CompletableFuture is already done (does not suspend)
  if (isDone) {
    try {
      @Suppress("UNCHECKED_CAST", "BlockingMethodInNonBlockingContext") return get() as T
    } catch (e: ExecutionException) {
      throw e.cause ?: e // unwrap original cause from ExecutionException
    }
  }
  // slow path -- suspend
  return suspendCancellableCoroutine { cont: CancellableContinuation<T> ->
    val consumer = ContinuationConsumer(cont)
    whenComplete(consumer)
    cont.invokeOnCancellation {
      cancel(false)
      consumer.cont = null // shall clear reference to continuation to aid GC
    }
  }
}

private class ContinuationConsumer<T>(@Volatile @JvmField var cont: Continuation<T>?) :
  KafkaFuture.BiConsumer<T?, Throwable?> {
  @Suppress("UNCHECKED_CAST")
  override fun accept(result: T?, exception: Throwable?) {
    val cont = this.cont ?: return // atomically read current value unless null
    if (exception == null) {
      // the future has completed normally
      cont.resume(result as T)
    } else {
      // the future has completed with an exception, unwrap it to provide consistent view of
      // .await() result and to propagate only original exception
      cont.resumeWithException((exception as? CompletionException)?.cause ?: exception)
    }
  }
}

/**
 * Converts this [KafkaFuture] to an instance of [Deferred].
 *
 * The [KafkaFuture] is cancelled when the resulting deferred is cancelled.
 */
@OptIn(InternalCoroutinesApi::class)
@Suppress("DeferredIsResult")
public fun <T> KafkaFuture<T>.asDeferred(): Deferred<T> {
  // Fast path if already completed
  if (isDone) {
    return try {
      @Suppress("UNCHECKED_CAST") (CompletableDeferred(get() as T))
    } catch (e: Throwable) {
      // unwrap original cause from ExecutionException
      val original = (e as? ExecutionException)?.cause ?: e
      CompletableDeferred<T>().also { it.completeExceptionally(original) }
    }
  }
  val result = CompletableDeferred<T>()
  whenComplete { value, exception ->
    try {
      if (exception == null) {
        // the future has completed normally
        result.complete(value)
      } else {
        // the future has completed with an exception, unwrap it consistently with fast path
        // Note: In the fast-path the implementation of CompletableFuture.get() does unwrapping
        result.completeExceptionally((exception as? CompletionException)?.cause ?: exception)
      }
    } catch (e: Throwable) {
      // We come here iff the internals of Deferred threw an exception during its completion
      handleCoroutineException(EmptyCoroutineContext, e)
    }
  }
  result.cancelFutureOnCompletion(this)
  return result
}
