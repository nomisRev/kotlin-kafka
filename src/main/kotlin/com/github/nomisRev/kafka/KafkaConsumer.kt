package com.github.nomisRev.kafka

import java.time.Duration
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener

public fun <K, V> kafkaConsumer(settings: ConsumerSettings<K, V>): Flow<KafkaConsumer<K, V>> =
    flow {
  KafkaConsumer(settings.properties(), settings.keyDeserializer, settings.valueDeserializer).use {
    emit(it)
  }
}

@OptIn(FlowPreview::class)
public fun <K, V> Flow<KafkaConsumer<K, V>>.subscribeTo(
  name: String,
  listener: ConsumerRebalanceListener = NoOpConsumerRebalanceListener(),
  timeout: Duration = Duration.ofMillis(500)
): Flow<ConsumerRecord<K, V>> = flatMapConcat { consumer ->
  flow {
    consumer.subscribe(listOf(name), listener)
    val job: Job? = kotlin.coroutines.coroutineContext[Job]
    while (true) {
      job?.ensureActive()
      consumer.poll(timeout).forEach { record -> emit(record) }
    }
  }
}
