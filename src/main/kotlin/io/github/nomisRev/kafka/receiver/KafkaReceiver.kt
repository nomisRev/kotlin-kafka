package io.github.nomisRev.kafka.receiver

import io.github.nomisRev.kafka.receiver.internals.PollLoop
import io.github.nomisRev.kafka.receiver.internals.kafkaScheduler
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

// TODO Copy name from reactor-kafka,
//  conflict with org.apache.kafka.clients.consumer.KafkaConsumer,
//  or figure out a other good name
public interface KafkaReceiver<K, V> {
  
  public fun receive(topicNames: Collection<String>): Flow<ReceiverRecord<K, V>>
  
  public fun receive(topicName: String): Flow<ReceiverRecord<K, V>> =
    receive(setOf(topicName))
  
  public fun receiveAutoAck(topicNames: Collection<String>): Flow<Flow<ConsumerRecord<K, V>>>
  
  public fun receiveAutoAck(topicNames: String): Flow<Flow<ConsumerRecord<K, V>>> =
    receiveAutoAck(setOf(topicNames))
  
  public suspend fun <A> withConsumer(action: suspend KafkaConsumer<K, V>.(KafkaConsumer<K, V>) -> A): A
}

@Suppress("FunctionName")
public fun <K, V> KafkaReceiver(settings: ReceiverSettings<K, V>): KafkaReceiver<K, V> =
  DefaultKafkaReceiver(settings)

@OptIn(FlowPreview::class)
private class DefaultKafkaReceiver<K, V>(private val settings: ReceiverSettings<K, V>) : KafkaReceiver<K, V> {
  
  fun kafkaConsumer(): Flow<KafkaConsumer<K, V>> =
    flow {
      KafkaConsumer(settings.toProperties(), settings.keyDeserializer, settings.valueDeserializer)
        .use { emit(it) }
    }
  
  override suspend fun <A> withConsumer(action: suspend KafkaConsumer<K, V>.(KafkaConsumer<K, V>) -> A): A =
    KafkaConsumer(settings.toProperties(), settings.keyDeserializer, settings.valueDeserializer)
      .use { action(it, it) }
  
  override fun receive(topicNames: Collection<String>): Flow<ReceiverRecord<K, V>> =
    kafkaScheduler(settings.groupId).flatMapConcat { (scope, dispatcher) ->
      kafkaConsumer().flatMapConcat { consumer ->
        val loop = PollLoop(topicNames, settings, consumer, scope)
        loop.receive().flowOn(dispatcher)
          .flatMapConcat { records ->
            records.map { record ->
              ReceiverRecord(record, loop.toCommittableOffset(record))
            }.asFlow()
          }
      }
    }
  
  override fun receiveAutoAck(topicNames: Collection<String>): Flow<Flow<ConsumerRecord<K, V>>> =
    kafkaScheduler(settings.groupId).flatMapConcat { (scope, dispatcher) ->
      kafkaConsumer().flatMapConcat { consumer ->
        val loop = PollLoop(topicNames, settings, consumer, scope)
        loop.receive().flowOn(dispatcher).map { records ->
          records.asFlow()
            .onCompletion { records.forEach { loop.toCommittableOffset(it).acknowledge() } }
        }
      }
    }
}
