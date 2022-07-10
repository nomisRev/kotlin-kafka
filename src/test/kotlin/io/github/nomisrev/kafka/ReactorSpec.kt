package io.github.nomisrev.kafka

import io.kotest.core.spec.style.StringSpec
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.asPublisher
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.time.Duration
import java.time.Instant

class ReactorSpec : StringSpec({
  val kafka = kafkaContainer()
  val topicName = "demo-topic"
  val count = 20
  val senderOptions: SenderOptions<Int, String> = SenderOptions.create(
    mapOf(
      BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
      CLIENT_ID_CONFIG to "sample-producer",
      ACKS_CONFIG to "all",
      KEY_SERIALIZER_CLASS_CONFIG to IntegerSerializer::class.java,
      VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
    )
  )
  val receiverOptions: ReceiverOptions<Int, String> = ReceiverOptions.create<Int, String>(
    mapOf(
      BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
      CLIENT_ID_CONFIG to "sample-consumer",
      GROUP_ID_CONFIG to "sample-group",
      KEY_DESERIALIZER_CLASS_CONFIG to IntegerDeserializer::class.java,
      VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
      AUTO_OFFSET_RESET_CONFIG to "earliest",
    )
  ).commitBatchSize(5).commitInterval(Duration.ofSeconds(15))
  val messages = (1..count).map { i ->
    SenderRecord.create(ProducerRecord(topicName, i, "Message_$i"), i)
  }
  
  "test" {
    listOf(
      async {
        KafkaSender.create(senderOptions).asFlow().flatMapConcat { sender ->
          sender.send(messages)
        }.collect { r: SenderResult<Int> ->
          val m = r.recordMetadata()
          val timestamp = Instant.ofEpochMilli(m.timestamp())
          println("Message ${r.correlationMetadata()} sent successfully, topic-partition=${m.topic()}-${m.partition()} offset=${m.offset()} timestamp=$timestamp")
        }
      },
      async {
        KafkaReceiver.create(receiverOptions.subscription(setOf(topicName)))
          .receive()
          .asFlow()
          .take(count)
          .collect { record: ReceiverRecord<Int, String> ->
            val offset = record.receiverOffset()
            val timestamp: Instant = Instant.ofEpochMilli(record.timestamp())
            println("Received message: topic-partition=${offset.topicPartition()} offset=${offset.offset()} timestamp=$timestamp key=${record.key()} value=${record.value()}")
            offset.acknowledge()
          }
      }).awaitAll()
  }
})

fun <K, V, T> KafkaSender<K, V>.send(records: Flow<SenderRecord<K, V, T>>): Flow<SenderResult<T>> =
  send(records.asPublisher()).asFlow()

fun <K, V, T> KafkaSender<K, V>.send(records: Iterable<SenderRecord<K, V, T>>): Flow<SenderResult<T>> =
  send(Flux.fromIterable(records)).asFlow()

fun <K, V> KafkaSender<K, V>.asFlow(): Flow<KafkaSender<K, V>> = flow {
  try {
    emit(this@asFlow)
  } finally {
    close()
  }
}
