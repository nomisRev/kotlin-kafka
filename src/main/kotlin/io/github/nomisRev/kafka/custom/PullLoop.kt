package io.github.nomisRev.kafka.custom

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import org.apache.kafka.clients.consumer.ConsumerRecords

// suspend tailrec fun <K, V> poll(
//   channel: Channel<ConsumerRecords<K, V>>
// ): Unit =