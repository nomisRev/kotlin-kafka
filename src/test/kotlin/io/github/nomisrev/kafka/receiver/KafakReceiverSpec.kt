package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisrev.kafka.KafkaSpec
import io.github.nomisrev.kafka.assertThrows
import io.github.nomisrev.kafka.mapIndexed
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.yield
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

@ExperimentalCoroutinesApi
class KafakReceiverSpec : KafkaSpec() {

  val count = 1000
  val lastIndex = count - 1
  fun produced(
    startIndex: Int = 0,
    lastIndex: Int = count,
  ): List<Pair<String, String>> =
    (startIndex until lastIndex).map { n -> Pair("key-$n", "value->$n") }

  val produced = produced()

  @Test
  fun `All produced records are received`() = withTopic {
    publishToKafka(topic, produced)
    assertEquals(
      KafkaReceiver(receiverSetting)
        .receive(topic.name())
        .map { record ->
          yield()
          Pair(record.key(), record.value())
            .also { record.offset.acknowledge() }
        }.take(count)
        .toSet(),
      produced.toSet()
    )
  }

  @Test
  fun `All produced records with headers are received`() = withTopic(partitions = 1) {
    val producerRecords = produced.map { (key, value) ->
      ProducerRecord(topic.name(), key, value).apply {
        headers().add("header1", byteArrayOf(0.toByte()))
        headers().add("header2", value.toByteArray())
      }
    }

    publishToKafka(producerRecords)

    KafkaReceiver(receiverSetting)
      .receive(topic.name())
      .take(count)
      .onEach { it.offset.acknowledge() }
      .toList().zip(producerRecords) { actual, expected ->
        assertEquals(actual.key(), expected.key())
        assertEquals(actual.value(), expected.value())
        assertEquals(actual.topic(), topic.name())
        assertEquals(actual.headers().toArray().size, 2)
        assertEquals(actual.headers(), expected.headers())
      }
  }

  @Test
  fun `Should receive all records when subscribing several consumers`() = withTopic {
    publishToKafka(topic, produced)
    val consumer =
      KafkaReceiver(receiverSetting)
        .receive(topic.name())
        .map {
          yield()
          Pair(it.key(), it.value())
        }

    assertEquals(
      flowOf(consumer, consumer)
        .flattenMerge()
        .take(count)
        .toSet(),
      produced.toSet()
    )
  }

  @Test
  fun `All acknowledged messages are committed on flow completion`() = withTopic {
    publishToKafka(topic, produced)
    val receiver = KafkaReceiver(
      receiverSetting.copy(
        commitStrategy = CommitStrategy.BySize(2 * count)
      )
    )
    receiver.receive(topic.name())
      .take(count)
      .collectIndexed { index, value ->
        if (index == lastIndex) {
          value.offset.acknowledge()
          assertEquals(receiver.committedCount(topic.name()), 0)
        } else value.offset.acknowledge()
      }

    assertEquals(receiver.committedCount(topic.name()), count.toLong())
  }

  @Test
  fun `All acknowledged messages are committed on flow failure`() = withTopic {
    publishToKafka(topic, produced)
    val receiver = KafkaReceiver(
      receiverSetting.copy(
        commitStrategy = CommitStrategy.BySize(2 * count)
      )
    )
    val exception = assertThrows<RuntimeException> {
      receiver.receive(topic.name())
        .collectIndexed { index, value ->
          if (index == lastIndex) {
            value.offset.acknowledge()
            assertEquals(receiver.committedCount(topic.name()), 0)
            throw boom
          } else value.offset.acknowledge()
        }
    }

    assertEquals(exception, boom)
    assertEquals(receiver.committedCount(topic.name()), count.toLong())
  }

  @Test
  fun `All acknowledged messages are committed on flow cancellation`() = withTopic {
    val scope = this
    publishToKafka(topic, produced)
    val receiver = KafkaReceiver(
      receiverSetting.copy(
        commitStrategy = CommitStrategy.BySize(2 * count)
      )
    )
    val latch = CompletableDeferred<Unit>()
    val job = receiver.receive(topic.name())
      .mapIndexed { index, value ->
        if (index == lastIndex) {
          value.offset.acknowledge()
          assertEquals(receiver.committedCount(topic.name()), 0)
          require(latch.complete(Unit)) { "Latch completed twice" }
        } else value.offset.acknowledge()
      }.launchIn(scope)

    latch.await()
    job.cancelAndJoin()

    assertEquals(receiver.committedCount(topic.name()), count.toLong())
  }

  @Test
  fun `Manual commit also commits all acknowledged offsets`() = withTopic {
    publishToKafka(topic, produced)
    val receiver = KafkaReceiver(
      receiverSetting.copy(
        commitStrategy = CommitStrategy.BySize(2 * count)
      )
    )
    receiver.receive(topic.name())
      .take(count)
      .collectIndexed { index, value ->
        if (index == lastIndex) {
          value.offset.commit()
          assertEquals(receiver.committedCount(topic.name()), count.toLong())
        } else value.offset.acknowledge()
      }
  }

  @Test
  fun receiveAutoAck() = withTopic {
    publishToKafka(topic, produced)
    val receiver = KafkaReceiver(receiverSetting)

    receiver.receiveAutoAck(topic.name())
      .flatMapConcat { it }
      .take(count)
      .collect()

    assertEquals(receiver.committedCount(topic.name()), count.toLong())
  }

  @Test
  fun `receiveAutoAck does not receive same records`() = withTopic {
    publishToKafka(topic, produced)
    val receiver = KafkaReceiver(receiverSetting)

    receiver.receiveAutoAck(topic.name())
      .flatMapConcat { it }
      .take(count)
      .collect()

    assertEquals(receiver.committedCount(topic.name()), count.toLong())

    val seconds = produced(count + 1, count + 1 + count)
    publishToKafka(topic, seconds)

    assertEquals(
      receiver.receiveAutoAck(topic.name())
        .flatMapConcat { it }
        .map { Pair(it.key(), it.value()) }
        .take(count)
        .toSet(),
      seconds.toSet()
    )

    assertEquals(receiver.committedCount(topic.name()), (2L * count))
  }
}
