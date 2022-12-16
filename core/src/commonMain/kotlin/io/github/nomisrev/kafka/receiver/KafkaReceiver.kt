package io.github.nomisrev.kafka.receiver

import io.github.nomisrev.kafka.Consumer
import io.github.nomisrev.kafka.kafkaScheduler
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow

public interface KafkaReceiver<K, V> {

  public fun receive(topicNames: Collection<String>): Flow<ReceiverRecord<K, V>>

  public fun receive(topicName: String): Flow<ReceiverRecord<K, V>> =
    receive(setOf(topicName))

//  public fun receiveAutoAck(topicNames: Collection<String>): Flow<Flow<ConsumerRecord<K, V>>>
//  public fun receiveAutoAck(topicNames: String): Flow<Flow<ConsumerRecord<K, V>>> = receiveAutoAck(setOf(topicNames))
//  public suspend fun <A> withConsumer(action: suspend KafkaConsumer<K, V>.(KafkaConsumer<K, V>) -> A): A
}

public expect fun <K, V> KafkaReceiver(settings: ReceiverSettings<K, V>): KafkaReceiver<K, V>

@OptIn(FlowPreview::class)
private class DefaultKafkaReceiver<K, V>(private val settings: ReceiverSettings<K, V>) : KafkaReceiver<K, V> {

  fun kafkaConsumer(): Flow<Consumer<K, V>> = io.github.nomisrev.kafka.kafkaConsumer(settings)

//  override suspend fun <A> withConsumer(action: suspend KafkaConsumer<K, V>.(KafkaConsumer<K, V>) -> A): A =
//    KafkaConsumer(settings.toProperties(), settings.keyDeserializer, settings.valueDeserializer)
//      .use { action(it, it) }

  override fun receive(topicNames: Collection<String>): Flow<ReceiverRecord<K, V>> =
    kafkaScheduler(settings.groupId).flatMapConcat { (scope, dispatcher) ->
      kafkaConsumer().flatMapConcat { consumer ->
//        val loop = PollLoop(topicNames, settings, consumer, scope)
//        loop.receive().flowOn(dispatcher)
//          .flatMapConcat { records ->
//            records.map { record ->
//              ReceiverRecord(record, loop.toCommittableOffset(record))
//            }.asFlow()
//          }
        TODO()
      }
    }

//  override fun receiveAutoAck(topicNames: Collection<String>): Flow<Flow<ConsumerRecord<K, V>>> =
//    kafkaScheduler(settings.groupId).flatMapConcat { (scope, dispatcher) ->
//      kafkaConsumer().flatMapConcat { consumer ->
//        val loop = PollLoop(topicNames, settings, consumer, scope)
//        loop.receive().flowOn(dispatcher).map { records ->
//          records.asFlow()
//            .onCompletion { records.forEach { loop.toCommittableOffset(it).acknowledge() } }
//        }
//      }
//    }
}
