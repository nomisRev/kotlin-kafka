package io.github.nomisRev.kafka.publisher

import io.github.nomisRev.kafka.Flow.KafkaPublisher
import io.github.nomisRev.kafka.publisher.DefaultKafkaPublisher.State.ACTIVE
import io.github.nomisRev.kafka.publisher.DefaultKafkaPublisher.State.COMPLETE
import io.github.nomisRev.kafka.publisher.DefaultKafkaPublisher.State.DONE_COLLECTING
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.invoke
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.internals.TransactionManager
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.toJavaDuration


/**
 * Reactive producer that sends messages to Kafka topic partitions. The producer is thread-safe
 * and can be used to send messages to multiple partitions. It is recommended that a single
 * producer is shared for each message type in a client.
 *
 * @param <K> outgoing message key type
 * @param <V> outgoing message value type
</V></K> */
class DefaultKafkaPublisher<Key, Value>(
  _createProducer: suspend () -> Producer<Key, Value>,
  val settings: PublisherSettings<Key, Value>,
  val onPublisherRecordDropped: suspend (ProducerRecord<Key, Value>) -> Unit =
    { log.debug("onNextDropped: {}", it) }
) : KafkaPublisher<Key, Value> {
  private val producerContext: ExecutorCoroutineDispatcher =
    Executors.newScheduledThreadPool(1) { runnable ->
      Thread(runnable, producerId).apply {
        isDaemon = true
      }
    }.asCoroutineDispatcher()

  @OptIn(ExperimentalCoroutinesApi::class, DelicateCoroutinesApi::class)
  val transactionContext: ExecutorCoroutineDispatcher? =
    settings.transactionalId()?.let { newSingleThreadContext(it) }

  private val transactionManager: TransactionManager? = null
  private val producerId: String = "reactor-kafka-sender-" + System.identityHashCode(this)

  @OptIn(DelicateCoroutinesApi::class)
  val producer = GlobalScope.async(producerContext) {
    withTransactionContext {
      _createProducer().apply {
        settings.producerListener.producerAdded(producerId, this)
        if (settings.isTransactional()) {
          log.info("Initializing transactions for producer {}", settings.transactionalId())
          initTransactions()
        }
      }
    }
  }

  enum class State { ACTIVE, DONE_COLLECTING, COMPLETE }

  suspend fun <A> withTransactionContext(
    block: suspend () -> A
  ): A = transactionContext?.invoke { block() } ?: block()

  override fun <T> send(records: Flow<PublisherRecord<Key, Value, T>>): Flow<RecordAck<T>> =
    doSend(records)

  private fun <A> doSend(records: Flow<ProducerRecord<Key, Value>>): Flow<RecordAck<A>> =
    channelFlow<RecordAck<A>> {
      val producer = producer.await()

      val inflight = AtomicInteger(0)
      val delayedThrowable = AtomicReference<Throwable?>(null)
      val state: AtomicReference<State> = AtomicReference<State>(ACTIVE)
      val ctx = currentCoroutineContext()
      val handler = ctx[CoroutineExceptionHandler]

      /*
       * **If** in DONE State we transition to COMPLETE.
       * We close the channelFlow so that the FlowCollector can complete.
       * In case of any errors, the FlowCollector will see non-null Throwable.
       */
      fun doneToComplete() {
        if (state.compareAndSet(DONE_COLLECTING, COMPLETE)) {
          val exception = delayedThrowable.get()
          close(exception)
        }
      }

      /*
       * **If** in ACTIVE State then we transition to DONE_COLLECTING the provided records,
       * and in-case all inflight records are already acknowledged then we also transition to COMPLETE.
       */
      fun activeToDoneOrCompleteIfAlreadyDone() {
        if (state.compareAndSet(ACTIVE, DONE_COLLECTING)) {
          if (inflight.get() == 0) {
            doneToComplete()
          }
        }
      }

      /*
       * Set COMPLETE State, and close the channelFlow so that the FlowCollector can complete.
       * If the channelFlow was already closed, we send the error to the `CoroutineExceptionHandler`,
       * which if null is set to the default logging CoroutineExceptionHandler.
       */
      fun complete(t: Throwable?) {
        log.trace("Sender failed with exception", t)
        state.getAndSet(COMPLETE)
        val alreadyClosed = !close(t)
        if (alreadyClosed) {
          t?.let { handler?.handleException(ctx, t) }
        }
      }

      records.collect { record: ProducerRecord<Key, Value> ->
        withTransactionContext {
          if (state.get() == COMPLETE) return@withTransactionContext onPublisherRecordDropped(record)

          inflight.incrementAndGet()
          if (settings.isTransactional()) {
            log.trace(
              "Transactional send initiated for producer {} in state {} inflight {}: {}",
              settings.transactionalId(),
              state,
              inflight,
              record
            )
          }

          val correlationMetadata: A =
            @Suppress("UNCHECKED_CAST")
            (record as PublisherRecord<Key, Value, A>).correlationMetadata

          val callback = Callback { metadata, exception ->
            if (settings.isTransactional()) {
              log.trace(
                "Transactional send completed for producer {} in state {} inflight {}: {}",
                settings.transactionalId(),
                state,
                inflight,
                record
              )
            }

            /* A fatal error occurred from a parallel record */
            if (state.get() === COMPLETE) {
              return@Callback
            }

            if (exception != null) {
              log.trace("Sender failed: ", exception)
              delayedThrowable.compareAndSet(null, exception)
              if (settings.stopOnError || settings.isFatal(exception)) {
                return@Callback complete(exception)
              } else {
                trySendBlocking(RecordAck.Failed(exception, correlationMetadata!!))
                  // If Channel is already closed, this exception was already seen before
                  //
                  .onFailure { close(it) }
              }
            } else {
              trySendBlocking(RecordAck.Success(metadata, correlationMetadata!!))
                // If Channel is already closed, this exception was already seen before
                .onFailure { close(it) }
            }

            doneToComplete()
          }

          try {
            runInterruptible { producer.send(record, callback) }
          } catch (e: Exception) {
            callback.onCompletion(null, e)
          }
        }
      }

      activeToDoneOrCompleteIfAlreadyDone()
      awaitClose { }
    }.flowOn(producerContext)

  override fun <T> sendTransitionally(records: Flow<Flow<PublisherRecord<Key, Value, T>>>): Flow<Flow<RecordAck<T>>> {
    TODO()
    //  val sink: Sinks.Many<Any> = Sinks.many().unicast().onBackpressureBuffer()
    //  return Flow.from(transactionRecords)
    //    .publishOn(settings.scheduler(), false, 1)
    //    .concatMapDelayError({ records -> transaction(records, sink) }, false, 1)
    //    .window(sink.asFlow())
    //    .doOnTerminate { sink.emitComplete(EmitFailureHandler.FAIL_FAST) }
    //    .doOnCancel { sink.emitComplete(EmitFailureHandler.FAIL_FAST) }
  }

  // TODO: Protect with lifecycle
  override suspend fun <T> withProducer(function: (Producer<Key, Value>) -> T): T =
    function(producer.await())

//  fun createOutbound(): KafkaOutbound<K, V> {
//    return DefaultKafkaOutbound(this)
//  }

  override fun transactionManager(): TransactionManager =
    requireNotNull(transactionManager) { "Transactions are not enabled" }

  override fun close(): Unit =
    if (!producer.isCompleted) Unit
    else {
      runBlocking(Dispatchers.IO) {
        withTransactionContext {
          val producer = producer.await()
          producer.close(
            if (settings.closeTimeout.isInfinite()) Duration.ofMillis(Long.MAX_VALUE)
            else settings.closeTimeout.toJavaDuration()
          )
          settings.producerListener.producerRemoved(producerId, producer)
        }
      }
      transactionContext?.close()
      producerContext.close()
    }

//  private fun <T> transaction(
//    transactionRecords: Publisher<out SenderRecord<K, V, T>?>,
//    transactionBoundary: Sinks.Many<Any>
//  ): Flow<PublisherResult<T>> {
//    return transactionManager()
//      .begin()
//      .thenMany(send<Any>(transactionRecords))
//      .concatWith(transactionManager().commit())
//      .concatWith(Mono.fromRunnable { transactionBoundary.emitNext(this, this) })
//      .onErrorResume { e -> transactionManager().abort().then(Mono.error(e)) }
//      .publishOn(settings.scheduler())
//  }

//  @Synchronized
//  private fun producerProxy(producer: Producer<K, V>): Producer<K, V>? {
//    if (this.producer == null) {
//      val interfaces = arrayOf<Class<*>>(Producer::class.java)
//      val handler =
//        InvocationHandler { proxy: Any?, method: Method, args: Array<Any?> ->
//          if (DELEGATE_METHODS.contains(method.name)) {
//            try {
//              return@InvocationHandler method.invoke(producer, *args)
//            } catch (e: InvocationTargetException) {
//              throw e.cause!!
//            }
//          } else {
//            throw UnsupportedOperationException("Method is not supported: $method")
//          }
//        }
//      this.producer = Proxy.newProxyInstance(
//        Producer::class.java.getClassLoader(),
//        interfaces,
//        handler
//      ) as Producer<K, V>
//    }
//    return this.producer
//  }

//  fun onEmitFailure(signalType: SignalType?, emitResult: Sinks.EmitResult?): Boolean {
//    return hasProducer.get()
//  }

  companion object {
    val log = LoggerFactory.getLogger(DefaultKafkaPublisher::class.java.getName())

    /** Note: Methods added to this set should also be included in javadoc for [KafkaPublisher.doOnProducer]  */
    private val DELEGATE_METHODS: Set<String> = HashSet(
      mutableListOf(
        "sendOffsetsToTransaction",
        "partitionsFor",
        "metrics",
        "flush"
      )
    )
  }
}

inline fun <V> AtomicReference<V>.loop(action: (V) -> Unit): Nothing {
  while (true) {
    action(get())
  }
}
