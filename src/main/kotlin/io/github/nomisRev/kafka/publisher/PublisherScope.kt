@file:JvmMultifileClass
@file:JvmName("PublisherScope.kt")

package io.github.nomisRev.kafka.publisher

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.ProducerFencedException
import kotlin.coroutines.cancellation.CancellationException

@DslMarker
annotation class PublisherDSL

@PublisherDSL
interface OfferScope<Key, Value> : CoroutineScope {
  suspend fun offer(record: ProducerRecord<Key, Value>)
  suspend fun offer(records: Iterable<ProducerRecord<Key, Value>>) =
    records.map { offer(it) }
}

@PublisherDSL
interface PublishScope<Key, Value> : OfferScope<Key, Value>, CoroutineScope {

  override suspend fun offer(record: ProducerRecord<Key, Value>)

  suspend fun publish(record: ProducerRecord<Key, Value>): RecordMetadata

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

@PublisherDSL
interface TransactionalScope<Key, Value> : PublishScope<Key, Value> {
  suspend fun <A> transaction(block: suspend PublishScope<Key, Value>.() -> A): A
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
