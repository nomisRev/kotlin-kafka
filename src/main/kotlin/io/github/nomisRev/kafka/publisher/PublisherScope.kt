@file:JvmMultifileClass @file:JvmName("PublisherScope.kt")

package io.github.nomisRev.kafka.publisher

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.ProducerFencedException

@DslMarker
public annotation class PublisherDSL

/**
 * The DSL, or receiver type, of [KafkaPublisher.publishScope] and [TransactionalScope.transaction].
 *
 * There are 2 main methods for sending recordings to kafka.
 *  - [offer], this gives you highest throughput and should generally be preferred.
 *  - [publish], this gives you less throughput, but waits on the delivery of the messages.
 */
@PublisherDSL
public interface PublishScope<Key, Value> : CoroutineScope {

  /**
   * Offer the [record] to Kafka, and immediately return.
   * This methods should be prepared for highest throughput,
   * if the [offer] fails it will cancel the [CoroutineScope] & [PublishScope].
   *
   * @param record to be offered to kafka
   */
  public suspend fun offer(record: ProducerRecord<Key, Value>)

  /**
   * Publisher a [record] to Kafka, and suspends until acknowledged by kafka.
   * This way of sending records to kafka results in a lower throughput.
   *
   * **IMPORTANT:** publish is slower than offer, if you need high throughput simply use [offer].
   *   Cancelling doesn't cancel the [publish]/[Producer.send].
   *
   * @param record to be delivered to kafka
   */
  public suspend fun publish(record: ProducerRecord<Key, Value>): RecordMetadata

  /**
   * Same as [offer], but for an [Iterable]] of [ProducerRecord].
   * @see offer
   */
  public suspend fun offer(records: Iterable<ProducerRecord<Key, Value>>) {
    records.map { offer(it) }
  }

  /**
   * Same as [publish], but for an [Iterable]] of [ProducerRecord].
   * @see publish
   */
  public suspend fun publish(record: Iterable<ProducerRecord<Key, Value>>): List<RecordMetadata> = coroutineScope {
    record.map { async { publish(it) } }.awaitAll()
  }

  /** Alias for `runCatching`, and `publish` except rethrows fatal exceptions */
  public suspend fun publishCatching(record: ProducerRecord<Key, Value>): Result<RecordMetadata>

  /**
   * Catch first failure of [publish], except fatal exceptions.
   * Alias for `runCatching`, `publish`, and `awaitAll`, except rethrows fatal exceptions
   */
  public suspend fun publishCatching(record: Iterable<ProducerRecord<Key, Value>>): Result<List<RecordMetadata>>
}

/**
 * The DSL, or receiver type, of [KafkaPublisher.publishScope],
 * it allows for publisher configured with [ProducerConfig.TRANSACTIONAL_ID_CONFIG],
 * and [ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] set to `true` to work transitionally.
 * Only one publisher with the same [ProducerConfig.TRANSACTIONAL_ID_CONFIG] can run at a given time.
 * An expired producer, still works but will throw [ProducerFencedException] if attempt to make a transaction again.
 *
 * In addition to [PublishScope] 2 main methods, a [TransactionalScope] also has the [transaction] method.
 *
 * It creates a **new** nested [PublishScope], and runs the lambda with the new scope as receiver.
 *
 * If the lambda succeeds, and the children of [Job] have completed,
 * it automatically commits the [transaction].
 *
 * In case the lambda fails, or any children of this [Job] fail,
 * then it'll be rethrown from this block and the transaction will be aborted.
 */
@PublisherDSL
public interface TransactionalScope<Key, Value> : PublishScope<Key, Value> {

  /**
   * Create and run a [transaction], which can [PublishScope.offer] and [PublishScope.publish] records to Kafka.
   * It awaits all inflight offers to finish, when successfully it commits the transaction and returns.
   * In case of failure
   *
   * If the [block] fails, or one of the children of the created [CoroutineScope],
   * then it aborts the transaction and the exception is rethrown and the [PublishScope] gets cancelled.
   *
   * Just like [coroutineScope] it awaits all its children to finish.
   *
   * ```kotlin
   * publisher.publishScope {
   *   transaction {
   *     // transaction { } compiler error: illegal to be called here
   *     offer((21..30).map {
   *       ProducerRecord(topic.name(), "$it", "msg-$it")
   *     })
   *     publish((31..40).map {
   *       ProducerRecord(topic.name(), "$it", "msg-$it")
   *     })
   *   }// Waits until all offer finished in transaction, fails if any failed
   * }
   * ```
   */
  public suspend fun <A> transaction(block: suspend PublishScope<Key, Value>.() -> A): A
}
