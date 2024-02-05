package io.github.nomisRev.kafka.publisher

import io.github.nomisRev.kafka.KafkaProducer
import io.github.nomisRev.kafka.publisher.DefaultKafkaPublisher.Companion.log
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
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.toJavaDuration

/**
 * Constructing a [KafkaPublisher] requires [PublisherSettings].
 * Optionally you can provide a different way to how the [Producer] is created.
 *
 * <!--- INCLUDE
 * import arrow.continuations.SuspendApp
 * import io.github.nomisRev.kafka.imap
 * import io.github.nomisRev.kafka.publisher.KafkaPublisher
 * import io.github.nomisRev.kafka.publisher.PublisherSettings
 * import org.apache.kafka.clients.producer.ProducerRecord
 * import org.apache.kafka.common.Metric
 * import org.apache.kafka.common.MetricName
 * import org.apache.kafka.common.serialization.IntegerSerializer
 * import org.apache.kafka.common.serialization.StringSerializer
 * @JvmInline value class Key(val index: Int)
 * @JvmInline value class Message(val content: String)
 * -->
 * ```kotlin
 * fun main() = SuspendApp {
 *   val settings = PublisherSettings(
 *     Kafka.container.bootstrapServers,
 *     IntegerSerializer().imap { key: Key -> key.index },
 *     StringSerializer().imap { msg: Message -> msg.content },
 *   )
 *
 *   KafkaPublisher(settings).use { publisher ->
 *     // ... use the publisher
 *     val m: Map<MetricName, Metric> = publisher.metrics()
 *
 *     publisher.publishScope {
 *       // send record without awaiting acknowledgement
 *       offer(ProducerRecord("example-topic", Key(1), Message("msg-1")))
 *
 *       // send record and suspends until acknowledged
 *       publish(ProducerRecord("example-topic", Key(2), Message("msg-2")))
 *     }
 *   }
 * }
 * ```
 * <!--- KNIT example-publisher-01.kt -->
 */
fun <Key, Value> KafkaPublisher(
  settings: PublisherSettings<Key, Value>,
  createProducer: (suspend (PublisherSettings<Key, Value>) -> Producer<Key, Value>)? = null
): KafkaPublisher<Key, Value> =
  DefaultKafkaPublisher(settings, createProducer)

/**
 * A [KafkaPublisher] wraps an [Producer], so needs to be closed by [AutoCloseable].
 * It has 1 main method, [publishScope] which creates a [PublishScope],
 * and two suspending methods from the [Producer] [partitionsFor], and [metrics].
 */
interface KafkaPublisher<Key, Value> : AutoCloseable {

  /**
   * Create and run a [publishScope], which can [PublishScope.offer] and [PublishScope.publish] records to Kafka.
   * It awaits all inflight offers to finish, and reports any errors.
   *
   * If the [block] fails, or one of the children of the created [CoroutineScope],
   * than the exception is rethrown and the [PublishScope] gets cancelled.
   *
   * Just like [coroutineScope] it awaits all its children to finish.
   *
   * ```kotlin
   * publisher.publishScope {
   *   offer((1..10).map {
   *     ProducerRecord(topic.name(), "$it", "msg-$it")
   *   })
   *   publish((11..20).map {
   *     ProducerRecord(topic.name(), "$it", "msg-$it")
   *   })
   *   transaction {
   *     // transaction { } compiler error: illegal to be called here
   *     offer((21..30).map {
   *       ProducerRecord(topic.name(), "$it", "msg-$it")
   *     })
   *     publish((31..40).map {
   *       ProducerRecord(topic.name(), "$it", "msg-$it")
   *     })
   *   }// Waits until all offer finished in transaction, fails if any failed
   *
   *   // streaming
   *   flow(1..100)
   *     .onEach { delay(100.milliseconds) }
   *     .map { ProducerRecord(topic.name(), "$it", "msg-$it") }
   *     .collect { offer(it) }
   * }
   * ```
   */
  suspend fun <A> publishScope(block: suspend TransactionalScope<Key, Value>.() -> A): A

  /** @see KafkaProducer.partitionsFor */
  suspend fun partitionsFor(topic: String): List<PartitionInfo>

  /** @see KafkaProducer.metrics */
  suspend fun metrics(): Map<MetricName, Metric>
}

private class DefaultKafkaPublisher<Key, Value>(
  val settings: PublisherSettings<Key, Value>,
  createProducer: (suspend (PublisherSettings<Key, Value>) -> Producer<Key, Value>)?
) : KafkaPublisher<Key, Value> {

  val producerId = "reactor-kafka-sender-${System.identityHashCode(this)}"
  val producerContext: ExecutorCoroutineDispatcher =
    Executors.newScheduledThreadPool(1) { runnable ->
      Thread(runnable, producerId).apply {
        isDaemon = true
      }
    }.asCoroutineDispatcher()

  @OptIn(DelicateCoroutinesApi::class)
  val producer = GlobalScope.async(producerContext) {
    val create = createProducer ?: settings.createProducer
    create(settings).apply {
      settings.producerListener.producerAdded(producerId, this)
      if (settings.isTransactional()) {
        log.info("Initializing transactions for producer {}", settings.transactionalId())
        initTransactions()
      }
    }
  }

  override suspend fun partitionsFor(topic: String): List<PartitionInfo> {
    val p = producer.await()
    return runInterruptible(producerContext) {
      p.partitionsFor(topic)
    }
  }


  override suspend fun metrics(): Map<MetricName, Metric> {
    val p = producer.await()
    return runInterruptible(producerContext) {
      p.metrics()
    }
  }

  override suspend fun <A> publishScope(block: suspend TransactionalScope<Key, Value>.() -> A): A {
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
    }
  }

  override fun close() = runBlocking {
    listOf(
      runCatching { settings.producerListener.producerRemoved(producerId, producer.await()) },
      runCatching {
        producer.await().close(
          if (settings.closeTimeout.isInfinite()) Duration.ofMillis(Long.MAX_VALUE)
          else settings.closeTimeout.toJavaDuration()
        )
      },
      runCatching { producerContext.close() }
    ).throwIfErrors()
  }

  companion object {
    val log: Logger = LoggerFactory.getLogger(KafkaPublisher::class.java.name)
  }
}

private fun Iterable<Result<Unit>>.throwIfErrors() {
  fold<Result<Unit>, Throwable?>(null) { acc, result ->
    acc?.apply {
      result.exceptionOrNull()?.let { addSuppressed(it) }
    } ?: result.exceptionOrNull()
  }?.let { throw it }
}

private class Token

private class DefaultProduceScope<Key, Value>(
  val settings: PublisherSettings<Key, Value>,
  val producer: Deferred<Producer<Key, Value>>,
  val producerContext: ExecutorCoroutineDispatcher,
  val token: Token,
  scope: CoroutineScope
) : TransactionalScope<Key, Value>, CoroutineScope by scope {
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

  override suspend fun publishCatching(record: ProducerRecord<Key, Value>): Result<RecordMetadata> =
    runCatching { publish(record) }.onFailure { settings.throwFatal(it) }

  override suspend fun publishCatching(record: Iterable<ProducerRecord<Key, Value>>): Result<List<RecordMetadata>> =
    runCatching {
      coroutineScope {
        record.map { async { publish(it) } }.awaitAll()
      }
    }.onFailure { settings.throwFatal(it) }

  override suspend fun <A> transaction(block: suspend PublishScope<Key, Value>.() -> A): A {
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

  private fun <Key, Value> PublisherSettings<Key, Value>.throwFatal(t: Throwable): Unit =
    if (
    // Fatal JVM errors
      t is VirtualMachineError ||
      t is ThreadDeath ||
      t is InterruptedException ||
      t is LinkageError ||
      // Fatal KotlinX error
      t is CancellationException ||
      // Fatal kafka errors
      isFatal(t)
    ) throw t else Unit
}

/*
 * Marker type, that this is our cancellation signal.
 * This allows us to check if it's our scope that is failing using [token].
 */
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

// Piggyback (copied) on KotlinX Coroutines DEBUG mechanism.
// https://github.com/Kotlin/kotlinx.coroutines/blob/ed0cf7aa02b1266cb81e65e61b3a97b0e041a817/kotlinx-coroutines-core/jvm/src/Debug.kt#L70
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
