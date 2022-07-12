package io.github.nomisrev.kafka.consumer

import io.github.nomisRev.kafka.consumer.internals.KConsumer
import io.github.nomisrev.kafka.KafkaSpec
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.yield

class ConsumerSpec : KafkaSpec({
  
  val depth = 5
  fun produced(): List<Pair<String, String>> =
    (0 until depth).map { n -> Pair("key-$n", "value->$n") }
  
  "should consume all records with subscribe" {
    val topic = createCustomTopic(partitions = 3)
    publishToKafka(topic, produced())
    KConsumer.subscribe(
      consumerSetting(),
      setOf(topic.name())
    ).map {
      yield()
      Pair(it.key(), it.value())
    }.take(depth).toList() shouldContainExactlyInAnyOrder produced()
  }
  
  "should consume all records at least once with subscribing for several consumers" {
    val topic = createCustomTopic(partitions = 3)
    publishToKafka(topic, produced())
    val consumer = KConsumer.subscribe(
      consumerSetting().copy(groupId = "test"),
      setOf(topic.name())
    ).map {
      yield()
      Pair(it.key(), it.value())
    }
    
    flowOf(consumer, consumer)
      .flattenMerge()
      .take(5)
      .toList() shouldContainExactlyInAnyOrder produced()
  }
  
  // Only receiving 3 records, 2 are stuck ???
  // TODO Improve API around subscribe vs assign
  // "should consume records with assign by partitions" {
  //   val topic = createCustomTopic(partitions = 3)
  //   publishToKafka(topic, produced())
  //   val partitions = listOf(1, 2, 3).map { partition ->
  //     TopicPartition(topic.name(), partition)
  //   }.toSortedSet(Comparator.comparingInt { it.partition() })
  //
  //   val settings = consumerSetting().copy(groupId = "test2")
  //   KafkaConsumer(settings).use { consumer ->
  //     consumer.assign(partitions)
  //     consumer.subscribe(settings, setOf())
  //       .map {
  //         yield()
  //         Pair(it.key(), it.value())
  //       }.take(depth).toList()
  //   } shouldContainExactlyInAnyOrder produced()
  // }
})
