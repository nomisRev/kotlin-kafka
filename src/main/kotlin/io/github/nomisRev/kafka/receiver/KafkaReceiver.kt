package io.github.nomisRev.kafka.receiver

import io.github.nomisRev.kafka.receiver.internals.PollLoop
import io.github.nomisRev.kafka.receiver.internals.kafkaScheduler
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import org.apache.kafka.clients.consumer.KafkaConsumer

// TODO Copy name from reactor-kafka,
//  conflict with org.apache.kafka.clients.consumer.KafkaConsumer,
//  or figure out a other good name
public interface KafkaReceiver<K, V> {
  
  public fun subscribe(topicNames: Collection<String>): Flow<ReceiverRecord<K, V>>
  
  public fun subscribe(topicName: String): Flow<ReceiverRecord<K, V>> =
    subscribe(setOf(topicName))
}

@Suppress("FunctionName")
public fun <K, V> KafkaReceiver(settings: ConsumerSettings<K, V>): KafkaReceiver<K, V> =
  DefaultKafkaReceiver(settings)

@OptIn(FlowPreview::class)
private class DefaultKafkaReceiver<K, V>(private val settings: ConsumerSettings<K, V>) : KafkaReceiver<K, V> {
  
  fun kafkaConsumer(): Flow<KafkaConsumer<K, V>> =
    flow {
      KafkaConsumer(settings.toProperties(), settings.keyDeserializer, settings.valueDeserializer)
        .use { emit(it) }
    }
  
  override fun subscribe(topicNames: Collection<String>): Flow<ReceiverRecord<K, V>> =
    kafkaScheduler(settings.groupId).flatMapConcat { (scope, dispatcher) ->
      kafkaConsumer().flatMapConcat { consumer ->
        val loop = PollLoop(topicNames, settings, consumer, scope)
        loop.receive().flatMapConcat { records ->
          records.map { record ->
            ReceiverRecord(record, loop.toCommittableOffset(record))
          }.asFlow()
        }.flowOn(dispatcher)
      }
    }
}
