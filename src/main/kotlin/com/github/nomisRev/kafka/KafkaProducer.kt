package com.github.nomisRev.kafka

import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.map
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

@FlowPreview
suspend fun <A, B> Flow<ProducerRecord<A, B>>.produce(
  settings: ProducerSettings<A, B>
): Flow<RecordMetadata> =
  settings.kafkaProducer().flatMapConcat { producer ->
    this@produce.map { record -> producer.sendAwait(record) }
  }

/**
 * Sends a record to a Kafka topic in a suspending way.
 *
 * ```kotlin
 * KafkaProducer(Properties(), StringSerializer(), StringSerializer()).use { producer ->
 *   producer.sendAwait(ProducerRecord("topic-name", "message #1"))
 *   producer.sendAwait(ProducerRecord("topic-name", "message #2"))
 * }
 * ```
 * TODO properly support interruptible `send`, whilst still working in a suspendig way.
 * ```
 *      See [kotlinx.coroutines.runInterruptible]
 * ```
 */
suspend fun <A, B> KafkaProducer<A, B>.sendAwait(record: ProducerRecord<A, B>): RecordMetadata =
    suspendCoroutine { cont ->
  send(record) { a, e ->
    // null if an error occurred, see: org.apache.kafka.clients.producer.Callback
    if (a != null) cont.resume(a) else cont.resumeWithException(e)
  }
}
