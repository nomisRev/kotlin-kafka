package io.github.nomisrev.kafka.consumer

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisrev.kafka.KafkaSpec
import io.github.nomisrev.kafka.mapIndexed
import io.kotest.assertions.assertSoftly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.shouldBe
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
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.timeout
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.yield
import org.apache.kafka.clients.producer.ProducerRecord
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class KafakReceiverSpec : KafkaSpec({

  val count = 1000
  val lastIndex = count - 1
  fun produced(
    startIndex: Int = 0,
    lastIndex: Int = count,
  ): List<Pair<String, String>> =
    (startIndex until lastIndex).map { n -> Pair("key-$n", "value->$n") }
  val produced = produced()

  "All produced records are received" {
    withTopic(partitions = 3) { topic ->
      publishToKafka(topic, produced)
      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .map { record ->
          yield()
          Pair(record.key(), record.value())
            .also { record.offset.acknowledge() }
        }.take(count)
        .toList() shouldContainExactlyInAnyOrder produced
    }
  }

  "All produced records with headers are received" {
    withTopic(partitions = 1) { topic ->
      val producerRecords = produced.map { (key, value) ->
        ProducerRecord(topic.name(), key, value).apply {
          headers().add("header1", byteArrayOf(0.toByte()))
          headers().add("header2", value.toByteArray())
        }
      }

      publishToKafka(producerRecords)

      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .take(count)
        .collectIndexed { index, received ->
          assertSoftly(producerRecords[index]) {
            received.key() shouldBe key()
            received.value() shouldBe value()
            received.topic() shouldBe topic.name()
            received.headers().toArray().size shouldBe 2
            received.headers() shouldBe headers()
          }
        }
    }
  }

  "Should receive all records when subscribing several consumers" {
    withTopic(partitions = 3) { topic ->
      publishToKafka(topic, produced)
      val consumer =
        KafkaReceiver(receiverSetting())
          .receive(topic.name())
          .map {
            yield()
            Pair(it.key(), it.value())
          }

      flowOf(consumer, consumer)
        .flattenMerge()
        .take(count)
        .toList() shouldContainExactlyInAnyOrder produced
    }
  }

  "All acknowledged messages are committed on flow completion" {
    withTopic(partitions = 3) { topic ->
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(
        receiverSetting().copy(
          commitStrategy = CommitStrategy.BySize(2 * count)
        )
      )
      receiver.receive(topic.name())
        .take(count)
        .collectIndexed { index, value ->
          if (index == lastIndex) {
            value.offset.acknowledge()
            receiver.committedCount(topic.name()) shouldBe 0
          } else value.offset.acknowledge()
        }

      receiver.committedCount(topic.name()) shouldBe count
    }
  }

  "All acknowledged messages are committed on flow failure" {
    withTopic(partitions = 3) { topic ->
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(
        receiverSetting().copy(
          commitStrategy = CommitStrategy.BySize(2 * count)
        )
      )
      val failure = RuntimeException("Flow terminates")
      runCatching {
        receiver.receive(topic.name())
          .collectIndexed { index, value ->
            if (index == lastIndex) {
              value.offset.acknowledge()
              receiver.committedCount(topic.name()) shouldBe 0
              throw failure
            } else value.offset.acknowledge()
          }
      }.exceptionOrNull() shouldBe failure

      receiver.committedCount(topic.name()) shouldBe count
    }
  }

  "All acknowledged messages are committed on flow cancellation" {
    val scope = this
    withTopic(partitions = 3) { topic ->
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(
        receiverSetting().copy(
          commitStrategy = CommitStrategy.BySize(2 * count)
        )
      )
      val latch = CompletableDeferred<Unit>()
      val job = receiver.receive(topic.name())
        .mapIndexed { index, value ->
          if (index == lastIndex) {
            value.offset.acknowledge()
            receiver.committedCount(topic.name()) shouldBe 0
            require(latch.complete(Unit)) { "Latch completed twice" }
          } else value.offset.acknowledge()
        }.launchIn(scope)

      latch.await()
      job.cancelAndJoin()

      receiver.committedCount(topic.name()) shouldBe count
    }
  }

  "Manual commit also commits all acknowledged offsets" {
    withTopic(partitions = 3) { topic ->
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(
        receiverSetting().copy(
          commitStrategy = CommitStrategy.BySize(2 * count)
        )
      )
      receiver.receive(topic.name())
        .take(count)
        .collectIndexed { index, value ->
          if (index == lastIndex) {
            value.offset.commit()
            receiver.committedCount(topic.name()) shouldBe count
          } else value.offset.acknowledge()
        }
    }
  }

  "receiveAutoAck" {
    withTopic(partitions = 3) { topic ->
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(receiverSetting())

      receiver.receiveAutoAck(topic.name())
        .flatMapConcat { it }
        .take(count)
        .collect()

      receiver.committedCount(topic.name()) shouldBe count
    }
  }

  "receiveAutoAck does not receive same records" {
    withTopic(partitions = 3) { topic ->
      publishToKafka(topic, produced)
      val receiver = KafkaReceiver(receiverSetting())

      receiver.receiveAutoAck(topic.name())
        .flatMapConcat { it }
        .take(count)
        .collect()

      receiver.committedCount(topic.name()) shouldBe count

      val seconds = produced(count + 1, count + 1 + count)
      publishToKafka(topic, seconds)

      receiver.receiveAutoAck(topic.name())
        .flatMapConcat { it }
        .map { Pair(it.key(), it.value()) }
        .take(count)
        .toList() shouldContainExactlyInAnyOrder seconds

      receiver.committedCount(topic.name()) shouldBe (2 * count)
    }
  }
})
