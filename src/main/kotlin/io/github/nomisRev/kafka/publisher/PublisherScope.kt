@file:JvmMultifileClass
@file:JvmName("PublisherScope.kt")

package io.github.nomisRev.kafka.publisher

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

@DslMarker
annotation class PublisherDSL

@PublisherDSL
interface PublishScope<Key, Value> : CoroutineScope {
  suspend fun offer(record: ProducerRecord<Key, Value>): Deferred<RecordMetadata>

  suspend fun publish(record: ProducerRecord<Key, Value>): RecordMetadata

  suspend fun publishCatching(record: ProducerRecord<Key, Value>): Result<RecordMetadata>

  suspend fun publishCatching(record: Iterable<ProducerRecord<Key, Value>>): Result<List<RecordMetadata>>

  suspend fun offer(records: Iterable<ProducerRecord<Key, Value>>): Deferred<List<RecordMetadata>> {
    val scope = this
    return scope.async { records.map { offer(it) }.awaitAll() }
  }

  suspend fun publish(record: Iterable<ProducerRecord<Key, Value>>): List<RecordMetadata> =
    coroutineScope {
      record.map { async { publish(it) } }.awaitAll()
    }

}

@PublisherDSL
interface TransactionalScope<Key, Value> : PublishScope<Key, Value> {
  suspend fun <A> transaction(block: suspend PublishScope<Key, Value>.() -> A): A
}
