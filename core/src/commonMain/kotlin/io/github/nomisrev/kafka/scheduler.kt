package io.github.nomisrev.kafka

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * [org.apache.kafka.clients.consumer.KafkaConsumer] is single-threaded,
 * and thus needs to have a dedicated [ExecutorCoroutineDispatcher] that guarantees a single thread
 * where we can schedule our interactions through [org.apache.kafka.clients.consumer.KafkaConsumer].
 *
 * This [Flow] returns a scheduler and CoroutineScope that is scoped to the stream,
 * it gets lazily initialized when the [Flow] is collected and gets closed when the flow terminates.
 */
internal expect fun kafkaScheduler(groupId: String): Flow<Pair<CoroutineScope, CoroutineDispatcher>>
