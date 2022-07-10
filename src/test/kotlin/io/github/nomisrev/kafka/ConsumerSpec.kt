package io.github.nomisrev.kafka

import io.github.nomisRev.kafka.Admin
import io.github.nomisRev.kafka.AdminSettings
import io.github.nomisRev.kafka.AutoOffsetReset
import io.github.nomisRev.kafka.ConsumerSettings
import io.github.nomisRev.kafka.NothingDeserializer
import io.github.nomisRev.kafka.NothingSerializer
import io.github.nomisRev.kafka.ProducerSettings
import io.github.nomisRev.kafka.createTopic
import io.github.nomisRev.kafka.internal.chunked
import io.kotest.assertions.timing.eventually
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.checkAll
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.apache.kafka.clients.CommonClientConfigs.*
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class ConsumerSpec : StringSpec({
  val kafka = kafkaContainer()
  val consumerGroupId = UUID.randomUUID().toString()
  val consumer by lazy {
    ConsumerSettings<String, Int>(
      bootstrapServers = kafka.bootstrapServers,
      groupId = consumerGroupId,
      autoOffsetReset = AutoOffsetReset.Earliest,
      keyDeserializer = StringDeserializer(),
      valueDeserializer = IntegerDeserializer()
    )
  }
  val producer by lazy { ProducerSettings(kafka.bootstrapServers, StringSerializer(), IntegerSerializer()) }
  val topicName = "topicName"
  beforeSpec {
    Admin(AdminSettings(kafka.bootstrapServers)).use { admin ->
      admin.createTopic(NewTopic(topicName, 1, 1))
    }
  }
  
  // "All received events are processed" {
  //   checkAll(
  //     20,
  //     Arb.map(
  //       Arb.string(minSize = 1),
  //       Arb.int()
  //     )
  //   ) { events ->
  //     events.map { (key, value) ->
  //       ProducerRecord(topicName, key, value)
  //     }.asFlow().produce(producer).collect()
  //
  //     buildMap {
  //       kafkaConsumer(consumer)
  //         .subscribeTo(topicName)
  //         .take(events.size)
  //         .map {
  //           it.also { (key, value) -> put(key, value) }
  //         }
  //         .commitBatchWithin(consumer, 1, 15.seconds)
  //         .collect()
  //     } shouldBe events
  //   }
  // }
  
  "All received events are processed" {
    checkAll(
      1,
      Arb.list(Arb.int())
    ) { events ->
      launch {
        val senderOptions = SenderOptions.create<Nothing, Int>(
          mapOf(
            BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
            CLIENT_ID_CONFIG to "sample-producer",
            ProducerConfig.ACKS_CONFIG to "all",
          )
        ).withKeySerializer(NothingSerializer)
          .withValueSerializer(IntegerSerializer())
        
        KafkaSender.create(senderOptions).asFlow().flatMapConcat { sender ->
          sender.send(events.map { value -> SenderRecord.create(ProducerRecord(topicName, value), value) })
        }.collect { r: SenderResult<Int> ->
          val m = r.recordMetadata()
          val timestamp = Instant.ofEpochMilli(m.timestamp())
          println("Message ${r.correlationMetadata()} sent successfully, topic-partition=${m.topic()}-${m.partition()} offset=${m.offset()} timestamp=$timestamp")
        }
      }
      
      buildList<Int> {
        val receiverOptions = ReceiverOptions.create<Nothing, String>(
          mapOf(
            BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
            CLIENT_ID_CONFIG to "sample-consumer",
            ConsumerConfig.GROUP_ID_CONFIG to "sample-group",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
          )
        ).commitBatchSize(5).commitInterval(Duration.ofSeconds(15))
          .withKeyDeserializer(NothingDeserializer)
          .withValueDeserializer(StringDeserializer())
        
        
        KafkaReceiver.create(receiverOptions.subscription(setOf(topicName)))
          .receive()
          .asFlow()
          .take(events.size)
          .collect { record: ReceiverRecord<Nothing, String> ->
            val offset = record.receiverOffset()
            val timestamp: Instant = Instant.ofEpochMilli(record.timestamp())
            println("Received message: topic-partition=${offset.topicPartition()} offset=${offset.offset()} timestamp=$timestamp key=${record.key()} value=${record.value()}")
            offset.acknowledge()
          }
      } shouldBe events
    }
  }
  
})
