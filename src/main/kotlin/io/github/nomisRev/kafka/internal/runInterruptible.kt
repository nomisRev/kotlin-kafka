package io.github.nomisRev.kafka.internal

import io.github.nomisRev.kafka.publisher.produce
import io.github.nomisRev.kafka.publisher.produceOrThrow
import kotlinx.coroutines.CompletionHandler
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.job
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.cancellation.CancellationException

/**
 * **DO NOT USE:**
 * This function is an internal implementation detail and should not be used directly.
 * It's only meant for guaranteeing collection of all in-flight records in [produce], and [produceOrThrow].
 *
 * Copied from: https://github.com/Kotlin/kotlinx.coroutines/blob/master/kotlinx-coroutines-core/jvm/src/Interruptible.kt#L40
 * but with [CoroutineStart.ATOMIC] so we can skip the cancellation check we get from `withContext`.
 */
internal suspend fun runInterruptibleAtomic(
  context: CoroutineContext = EmptyCoroutineContext,
  block: () -> Unit
): Unit = try {
  GlobalScope.async(context, start = CoroutineStart.ATOMIC) {
    runInterruptibleInExpectedContext(coroutineContext, block)
  }.await()
} catch (e: CancellationException) {
  // We see this exception, when the
}

private fun <T> runInterruptibleInExpectedContext(coroutineContext: CoroutineContext, block: () -> T): T {
  try {
    val threadState = ThreadState(coroutineContext.job)
    threadState.setup()
    try {
      return block()
    } finally {
      threadState.clearInterrupt()
    }
  } catch (e: InterruptedException) {
    throw CancellationException("Blocking call was interrupted due to parent cancellation").initCause(e)
  }
}

private const val WORKING = 0
private const val FINISHED = 1
private const val INTERRUPTING = 2
private const val INTERRUPTED = 3

private class ThreadState(private val job: Job) : CompletionHandler {
  /*
     === States ===

     WORKING: running normally
     FINISH: complete normally
     INTERRUPTING: canceled, going to interrupt this thread
     INTERRUPTED: this thread is interrupted

     === Possible Transitions ===

     +----------------+         register job       +-------------------------+
     |    WORKING     |   cancellation listener    |         WORKING         |
     | (thread, null) | -------------------------> | (thread, cancel handle) |
     +----------------+                            +-------------------------+
             |                                                |   |
             | cancel                                  cancel |   | complete
             |                                                |   |
             V                                                |   |
     +---------------+                                        |   |
     | INTERRUPTING  | <--------------------------------------+   |
     +---------------+                                            |
             |                                                    |
             | interrupt                                          |
             |                                                    |
             V                                                    V
     +---------------+                              +-------------------------+
     |  INTERRUPTED  |                              |         FINISHED        |
     +---------------+                              +-------------------------+
  */
  private val _state = AtomicInteger(WORKING)
  private val targetThread = Thread.currentThread()

  // Registered cancellation handler
  private var cancelHandle: DisposableHandle? = null

  @OptIn(InternalCoroutinesApi::class)
  fun setup() {
    cancelHandle = job.invokeOnCompletion(onCancelling = true, invokeImmediately = true, handler = this)
    // Either we successfully stored it or it was immediately cancelled
    _state.loop { state ->
      when (state) {
        // Happy-path, move forward
        WORKING -> if (_state.compareAndSet(state, WORKING)) return
        // Immediately cancelled, just continue
        INTERRUPTING, INTERRUPTED -> return
        else -> invalidState(state)
      }
    }
  }

  fun clearInterrupt() {
    /*
     * Do not allow to untriggered interrupt to leak
     */
    _state.loop { state ->
      when (state) {
        WORKING -> if (_state.compareAndSet(state, FINISHED)) {
          cancelHandle?.dispose()
          return
        }

        INTERRUPTING -> {
          /*
           * Spin, cancellation mechanism is interrupting our thread right now
           * and we have to wait it and then clear interrupt status
           */
        }

        INTERRUPTED -> {
          // Clear it and bail out
          Thread.interrupted()
          return
        }

        else -> invalidState(state)
      }
    }
  }

  // Cancellation handler
  override fun invoke(cause: Throwable?) {
    _state.loop { state ->
      when (state) {
        // Working -> try to transite state and interrupt the thread
        WORKING -> {
          if (_state.compareAndSet(state, INTERRUPTING)) {
            targetThread.interrupt()
            _state.set(INTERRUPTED)
            return
          }
        }
        // Finished -- runInterruptible is already complete, INTERRUPTING - ignore
        FINISHED, INTERRUPTING, INTERRUPTED -> return
        else -> invalidState(state)
      }
    }
  }

  inline fun AtomicInteger.loop(block: (state: Int) -> Unit) {
    while (true) {
      block(get())
    }
  }

  private fun invalidState(state: Int): Nothing = error("Illegal state $state")
}

