package io.github.nomisRev.kafka.publisher

import io.github.nomisRev.kafka.publisher.DefaultProduceScope.Companion.log
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CopyableThrowable
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DEBUG_PROPERTY_NAME
import kotlinx.coroutines.DEBUG_PROPERTY_VALUE_AUTO
import kotlinx.coroutines.DEBUG_PROPERTY_VALUE_OFF
import kotlinx.coroutines.DEBUG_PROPERTY_VALUE_ON
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.ProducerFencedException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.toJavaDuration

interface PublisherScope<Key, Value> : CoroutineScope {

  suspend fun offer(record: ProducerRecord<Key, Value>)

  suspend fun publish(record: ProducerRecord<Key, Value>): RecordMetadata

  suspend fun <A> transaction(block: suspend PublisherScope<Key, Value>.() -> A): A

  suspend fun offer(records: Iterable<ProducerRecord<Key, Value>>) =
    records.map { offer(it) }

  suspend fun publish(record: Iterable<ProducerRecord<Key, Value>>): List<RecordMetadata> =
    coroutineScope {
      record.map { async { publish(it) } }.awaitAll()
    }

  suspend fun publishCatching(record: ProducerRecord<Key, Value>): Result<RecordMetadata> =
    runCatching { publish(record) }.onFailure { throwFatal(it) }

  suspend fun publishCatching(record: Iterable<ProducerRecord<Key, Value>>): Result<List<RecordMetadata>> =
    runCatching {
      coroutineScope {
        record.map { async { publish(it) } }.awaitAll()
      }
    }.onFailure { throwFatal(it) }
}

suspend fun <Key, Value, A> publish(
  settings: PublisherSettings<Key, Value>,
  createProducer: suspend () -> Producer<Key, Value> =
    { KafkaProducer(settings.properties(), settings.keySerializer, settings.valueSerializer) },
  block: suspend PublisherScope<Key, Value>.() -> A
): A {
  val producerId = "reactor-kafka-sender-${UUID.randomUUID()}"
  val producerContext: ExecutorCoroutineDispatcher =
    Executors.newScheduledThreadPool(1) { runnable ->
      Thread(runnable, producerId).apply {
        isDaemon = true
      }
    }.asCoroutineDispatcher()

  @OptIn(DelicateCoroutinesApi::class)
  val producer = GlobalScope.async(producerContext) {
    createProducer().apply {
      settings.producerListener.producerAdded(producerId, this)
      if (settings.isTransactional()) {
        log.info("Initializing transactions for producer {}", settings.transactionalId())
        initTransactions()
      }
    }
  }
  val token = Token()
  return try {
    coroutineScope {
      val scope = DefaultProduceScope(
        settings,
        producer,
        producerContext,
        token,
        this
      )
      block(scope)
    }
  } catch (e: ChildCancelScope) {
    e.checkMyScope(token)
  } finally {
    withContext(NonCancellable) {
      listOf(
        runCatching {
          producer.await().close(
            if (settings.closeTimeout.isInfinite()) Duration.ofMillis(Long.MAX_VALUE)
            else settings.closeTimeout.toJavaDuration()
          )
        },
        runCatching { settings.producerListener.producerRemoved(producerId, producer.await()) },
        runCatching { producerContext.close() }
      ).throwIfErrors()
    }
  }
}

private class Token

private class DefaultProduceScope<Key, Value>(
  val settings: PublisherSettings<Key, Value>,
  val producer: Deferred<Producer<Key, Value>>,
  val producerContext: ExecutorCoroutineDispatcher,
  val token: Token,
  scope: CoroutineScope
) : PublisherScope<Key, Value>, CoroutineScope by scope {
  val parent: Job = requireNotNull(coroutineContext[Job]) { "Impossible, can only be called within coroutineScope" }

  override suspend fun offer(record: ProducerRecord<Key, Value>) {
    val p: Producer<Key, Value> = producer.await()
    val child = Job(parent)
    runInterruptible(producerContext) {
      p.send(record) { _, exception ->
        if (exception != null) parent.cancel(ChildCancelScope("Child failed", exception, token))
        else child.complete()
      }
    }
  }

  override suspend fun publish(record: ProducerRecord<Key, Value>): RecordMetadata {
    val p: Producer<Key, Value> = producer.await()
    val promise = CompletableDeferred<RecordMetadata>()
    runInterruptible(producerContext) {
      p.send(record) { metadata, exception ->
        if (exception != null) promise.completeExceptionally(exception)
        else promise.complete(metadata)
      }
    }
    return promise.await()
  }


  override suspend fun <A> transaction(block: suspend PublisherScope<Key, Value>.() -> A): A {
    val token = Token()
    val p = producer.await()
    withContext(producerContext) { p.beginTransaction() }
    log.debug("Begin a new transaction for producer {}", settings.transactionalId())
    return try {
      coroutineScope {
        val scope = DefaultProduceScope(
          settings,
          producer,
          producerContext,
          token,
          this
        )
        block(scope)
      }.also {
        withContext(producerContext) { p.commitTransaction() }
        log.debug("Commit current transaction for producer {}", settings.transactionalId())
      }
    } catch (e: Throwable) {
      withContext(producerContext) { p.abortTransaction() }
      log.debug("Abort current transaction for producer {}", settings.transactionalId())
      if (e is ChildCancelScope) e.checkMyScope(token) else throw e
    }
  }

  companion object {
    val log: Logger = LoggerFactory.getLogger(DefaultProduceScope::class.java.name)
  }
}

private fun Iterable<Result<Unit>>.throwIfErrors() {
  fold<Result<Unit>, Throwable?>(null) { acc, result ->
    acc?.apply {
      result.exceptionOrNull()?.let { addSuppressed(it) }
    } ?: result.exceptionOrNull()
  }?.let { throw it }
}

@OptIn(ExperimentalCoroutinesApi::class)
private class ChildCancelScope(
  message: String,
  override val cause: Throwable,
  @Transient @JvmField val token: Token,
) : CancellationException(message), CopyableThrowable<ChildCancelScope> {
  init {
    initCause(cause)
  }

  /*
   * In non-debug mode we don't want to have a stacktrace on every cancellation/close, parent job reference is enough.
   * Stacktrace of JCE is not needed most of the time (e.g., it is not logged) and hurts performance.
   */
  override fun fillInStackTrace(): Throwable =
    if (DEBUG) super.fillInStackTrace()
    else apply {
      stackTrace = emptyArray() // Prevent Android <= 6.0 bug, #1866
    }

  /* In non-debug mode we don't copy JCE for speed as it does not have the stack trace anyway. */
  override fun createCopy(): ChildCancelScope? =
    if (DEBUG) ChildCancelScope(message!!, cause, token)
    else null

  fun checkMyScope(scope: Token): Nothing =
    when {
      this.token === scope -> throw cause
      else -> throw this
    }
}

private fun throwFatal(t: Throwable): Unit =
  when (t) {
    // Fatal JVM errors
    is VirtualMachineError,
    is ThreadDeath,
    is InterruptedException,
    is LinkageError,
      // Fatal KotlinX error
    is CancellationException,
      // Fatal kafka errors
    is AuthenticationException,
    is ProducerFencedException -> throw t

    else -> Unit
  }

private val ASSERTIONS_ENABLED = ChildCancelScope::class.java.desiredAssertionStatus()

private val DEBUG = try {
  System.getProperty(DEBUG_PROPERTY_NAME)
} catch (e: SecurityException) {
  null
}.let { value ->
  when (value) {
    DEBUG_PROPERTY_VALUE_AUTO, null -> ASSERTIONS_ENABLED
    DEBUG_PROPERTY_VALUE_ON, "" -> true
    DEBUG_PROPERTY_VALUE_OFF -> false
    else -> error("System property '$DEBUG_PROPERTY_NAME' has unrecognized value '$value'")
  }
}
