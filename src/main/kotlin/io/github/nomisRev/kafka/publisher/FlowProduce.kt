package io.github.nomisRev.kafka.publisher

import io.github.nomisRev.kafka.internal.runInterruptibleAtomic
import io.github.nomisRev.kafka.publisher.ProducerFlowState.ACTIVE
import io.github.nomisRev.kafka.publisher.ProducerFlowState.COMPLETE
import io.github.nomisRev.kafka.publisher.ProducerFlowState.DONE_COLLECTING
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.toJavaDuration

/**
 * produce will send message to Kafka, and stream [Result] of [RecordMetadata] back to the user.
 * It will not stop sending messages if any error occurs,
 * you can throw it in the collector if you want the stream to stop. Otherwise, use [produceOrThrow].
 * Any encountered errors will be sent to the collector as [Result.failure],
 * and they will also be rethrown if the Flow completes without handling them (e.g. using `Flow.catch`).
 *
 * It'll wait acknowledgements from Kafka in **a different coroutine**, or asynchronously.
 *
 * This means we don't have to wait for the acknowledgement before sending the next message,
 * resulting in maximum throughput but still guarantees that the message was sent to Kafka.
 *
 * @param onPublisherRecordDropped a callback to handle dropped records, by default it uses the one from [PublisherSettings].
 *                 This only occurs when a fatal error occurred, and Flow transitions to COMPLETE.
 * @param createProducer a callback to create a producer, by default it uses the one from [PublisherSettings].
 */
fun <Key, Value> Flow<ProducerRecord<Key, Value>>.produce(
  settings: PublisherSettings<Key, Value>,
  onPublisherRecordDropped: (suspend (Logger, ProducerRecord<Key, Value>) -> Unit)? = null,
  createProducer: (suspend (PublisherSettings<Key, Value>) -> Producer<Key, Value>)? = null
): Flow<Result<RecordMetadata>> =
  produceImpl(
    settings,
    false,
    onPublisherRecordDropped,
    createProducer
  )

/**
 * produceOrThrow will send message to Kafka, and throw an exception if any error occurs.
 * To handle any errors, use `Flow.catch` to prevent the flow from being cancelled.
 * It'll wait acknowledgements from Kafka in **a different coroutine**, or asynchronously.
 *
 * This means we don't have to wait for the acknowledgement before sending the next message,
 * resulting in maximum throughput but still guarantees that the message was sent to Kafka.
 */
fun <Key, Value> Flow<ProducerRecord<Key, Value>>.produceOrThrow(
  settings: PublisherSettings<Key, Value>,
  onPublisherRecordDropped: (suspend (Logger, ProducerRecord<Key, Value>) -> Unit)? = null,
  createProducer: (suspend (PublisherSettings<Key, Value>) -> Producer<Key, Value>)? = null
): Flow<RecordMetadata> =
  produceImpl(settings, true, onPublisherRecordDropped, createProducer)
    // We know that this Flow will only emit `Result.success` so we can safely unwrap it
    .map { it.getOrThrow() }

private val log: Logger = LoggerFactory.getLogger(KafkaPublisher::class.java.name)

private enum class ProducerFlowState { ACTIVE, DONE_COLLECTING, COMPLETE }

/**
 * This is a private function that is used by both `produce` and `produceOrThrow`.
 *
 * It collects the upstream [ProducerRecord], and sends them to Kafka.
 * Awaiting the acknowledgement from Kafka in a Channel, and then sends the [RecordMetadata] downstream.
 * This is done, such that we don't need to await the acknowledgement before sending the next message,
 * and can guarantee the highest throughput whilst still guaranteeing that the messages were sent to Kafka.
 *
 * The state of the producer flow exists out of [ProducerFlowState]: [ACTIVE], [DONE_COLLECTING], and [COMPLETE].
 *
 * When we start `collecting`, the [Flow] will start sending records to Kafka.
 *
 * If [stopOnError] is true any errors will transition the [Flow] to [COMPLETE],
 * and the [Flow] will stop sending records to Kafka.
 *   => Any inflight records will not be dropped, but the resulting [RecordMetadata] will be dropped.
 *   => The error is immediately sent to the collector, and the [Flow] will rethrow the error.*
 * Otherwise, the [Flow] will continue sending records to Kafka,
 * and any error will be delayed until the [Flow] is done collecting records.
 * All results are send downstream as [Result], which can be safely unwrapped when `stopOnError == true`.
 * Otherwise, you'll have to handle the [Result] in the collector.
 *
 * In case of cancellation, we immediately transition to [COMPLETE].
 *
 * If the upstream finishes emitting records, we transition to [DONE_COLLECTING].
 * When all acknowledgements are received, we transition to [COMPLETE].
 *
 * ```
 * +-----+ collect +--------+ complete   +----------------+  finish  +-----------+
 * | New | -----> | Active | ---------> | DONE_COLLECTING | -------> | COMPLETE |
 * +-----+        +--------+            +-----------------+          +---+-------+
 *                  |  cancel & if stopOnError fail                     |
 *                  +---------------------------------------------------+
 * ```
 */
private fun <Key, Value> Flow<ProducerRecord<Key, Value>>.produceImpl(
  settings: PublisherSettings<Key, Value>,
  stopOnError: Boolean,
  onPublisherRecordDropped: (suspend (Logger, ProducerRecord<Key, Value>) -> Unit)?,
  createProducer: (suspend (PublisherSettings<Key, Value>) -> Producer<Key, Value>)?
): Flow<Result<RecordMetadata>> = channelFlow {
  val producerId = "reactor-kafka-sender-${System.identityHashCode(this)}"
  val producerContext: ExecutorCoroutineDispatcher =
    Executors.newScheduledThreadPool(1) { runnable ->
      Thread(runnable, producerId).apply {
        isDaemon = true
      }
    }.asCoroutineDispatcher()

  val producer = withContext(producerContext) {
    val create = createProducer ?: settings.createProducer
    create(settings).apply {
      settings.producerListener.producerAdded(producerId, this)
      if (settings.isTransactional()) {
        log.info("Initializing transactions for producer {}", settings.transactionalId())
        initTransactions()
      }
    }
  }

  val inflight = AtomicInteger(0)
  val delayedThrowable = AtomicReference<List<Throwable>?>(null)
  val state = AtomicReference(ACTIVE)
  val ctx = currentCoroutineContext()
  val handler = ctx[CoroutineExceptionHandler]

  /*
   * **If** in DONE State we transition to COMPLETE.
   * We close the channelFlow so that the FlowCollector can complete.
   * In case of any errors, the FlowCollector will see non-null Throwable.
   */
  fun doneToComplete() {
    if (inflight.get() == 0 && state.compareAndSet(DONE_COLLECTING, COMPLETE)) {
      close(delayedThrowable.getAll(null))
    }
  }

  /*
   * **If** in ACTIVE State then we transition to DONE_COLLECTING the provided records,
   * and in-case all inflight records are already acknowledged then we also transition to COMPLETE.
   */
  fun activeToDoneOrCompleteIfAlreadyDone() {
    if (state.compareAndSet(ACTIVE, DONE_COLLECTING)) {
        doneToComplete()
    }
  }

  /*
   * Set COMPLETE State, and close the channelFlow so that the FlowCollector can complete.
   * If the channelFlow was already closed, we send the error to the `CoroutineExceptionHandler`,
   * which is null is set to the default logging CoroutineExceptionHandler.
   */
  fun complete(errorOrNull: Throwable?) {
    val e = delayedThrowable.getAll(errorOrNull)
    log.trace("Sender failed with exception", e)
    state.getAndSet(COMPLETE)
    val alreadyClosed = !close(e)
    if (alreadyClosed && e != null) {
      handler?.handleException(ctx, e)
    }
  }

  fun delayError(exception: Throwable) {
    if (exception is CancellationException) return
    delayedThrowable.getAndUpdate { current ->
      if (current == null) listOf(exception) else current + exception
    }
  }

  fun logTransaction(record: ProducerRecord<Key, Value>, state: String) =
    if (settings.isTransactional()) {
      log.trace(
        "Transactional send completed for producer {} in state {} inflight {}: {}",
        settings.transactionalId(),
        state,
        inflight,
        record
      )
    } else Unit

  catch { exception ->
    when {
      exception is CancellationException -> Unit
      stopOnError || settings.isFatal(exception) -> complete(exception)
      else -> delayError(exception)
    }
  }.collect { record: ProducerRecord<Key, Value> ->
    if (state.get() == COMPLETE) {
      val dropped = onPublisherRecordDropped ?: settings.onPublisherRecordDropped
      return@collect dropped(log, record)
    }

    logTransaction(record, "initiated")

    val callback = Callback { metadata, exception ->
      logTransaction(record, "completed")
      inflight.decrementAndGet()
      when {
        state.get() === COMPLETE || isClosedForSend ->
          complete(IllegalStateException("state.get() === COMPLETE || isClosedForSend: bug in FlowProduce. Contact maintainers."))
        exception != null -> {
          log.trace("Sender failed: ", exception)
          when {
            exception is CancellationException -> Unit
            stopOnError || settings.isFatal(exception) -> complete(null)
            else -> {
              delayError(exception)
              trySendBlocking(Result.failure(exception)).getOrThrow()
              doneToComplete()
            }
          }
        }

        else -> {
          trySendBlocking(Result.success(metadata)).getOrThrow()
          doneToComplete()
        }
      }
    }

    try {
      runInterruptibleAtomic(producerContext) {
        inflight.incrementAndGet()
        producer.send(record, callback)
      }
    } catch (e: Exception) {
      callback.onCompletion(null, e)
    }
  }
  activeToDoneOrCompleteIfAlreadyDone()
  awaitClose {
    listOf(
      runCatching { settings.producerListener.producerRemoved(producerId, producer) },
      runCatching {
        producer.close(
          if (settings.closeTimeout.isInfinite()) Duration.ofMillis(Long.MAX_VALUE)
          else settings.closeTimeout.toJavaDuration()
        )
      },
      runCatching { producerContext.close() }
    ).throwIfErrors()
  }
}

private fun Iterable<Result<Unit>>.throwIfErrors() {
  fold<Result<Unit>, Throwable?>(null) { acc, result ->
    acc.add(result.exceptionOrNull())
  }?.let { throw it }
}

private fun Throwable?.add(other: Throwable?): Throwable? =
  this?.apply {
    other?.let { addSuppressed(it) }
  } ?: other

private fun AtomicReference<List<Throwable>?>.getAll(other: Throwable?): Throwable? =
  get()?.fold(other) { acc, result ->
    acc.add(result)
  }
