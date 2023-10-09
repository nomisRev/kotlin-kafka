package io.github.nomisrev.kafka.publisher

import io.github.nomisRev.kafka.publisher.Acks
import io.github.nomisRev.kafka.publisher.publish
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisrev.kafka.KafkaSpec
import io.kotest.assertions.async.shouldTimeout
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart.UNDISPATCHED
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

class KafkaPublisherSpec : KafkaSpec({

  "Can receive all messages that were offered on the right partitions" {
    withTopic(partitions = 4) { topic ->
      val count = 3
      val records = (0..count).map {
        topic.createProducerRecord(it)
      }
      publish(publisherSettings()) {
        offer(records)
      }

      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .map { record ->
          Pair(record.partition(), listOf(record.value()))
            .also { record.offset.acknowledge() }
        }
        .take(count + 1)
        .toList()
        .toMap() shouldBe records.groupBy({ it.partition() }) { it.value() }
    }
  }

  "Can receive all messages that were published on the right partitions" {
    withTopic(partitions = 4) { topic ->
      val count = 3
      val records = (0..count).map {
        topic.createProducerRecord(it)
      }
      publish(publisherSettings()) {
        publish(records)
      }

      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .map { record ->
          Pair(record.partition(), listOf(record.value()))
            .also { record.offset.acknowledge() }
        }
        .take(count + 1)
        .toList()
        .toMap() shouldBe records.groupBy({ it.partition() }) { it.value() }
    }
  }

  "A failure in a produce block, rethrows the error" {
    withTopic(partitions = 4) { topic ->
      val boom = RuntimeException("Boom!")
      val record = topic.createProducerRecord(0)

      shouldThrow<RuntimeException> {
        publish(publisherSettings()) {
          offer(record)
          throw boom
        }
      } shouldBe boom

      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .map {
          it.apply { offset.acknowledge() }
        }.first().value() shouldBe record.value()
    }
  }

  "A failure in a produce block with a concurrent launch cancels the launch, rethrows the error" {
    withTopic(partitions = 4) { topic ->
      val boom = RuntimeException("Boom!")
      val cancelSignal = CompletableDeferred<CancellationException>()
      shouldThrow<RuntimeException> {
        publish(publisherSettings()) {
          launch(start = UNDISPATCHED) {
            try {
              awaitCancellation()
            } catch (e: CancellationException) {
              cancelSignal.complete(e)
              throw e
            }
          }
          throw boom
        }
      } shouldBe boom
      cancelSignal.await()
    }
  }

  "A failed offer is rethrow at the end" {
    withTopic(partitions = 4) { topic ->
      val boom = RuntimeException("Boom!")
      val record = topic.createProducerRecord(0)
      val failingProducer = producer.stubProducer(_sendCallback = { metadata, callback ->
        if (metadata.key().equals("0")) {
          Executors.newScheduledThreadPool(1).schedule(
            {
              callback.onCompletion(null, boom)
            },
            1,
            TimeUnit.SECONDS
          )

          CompletableFuture.supplyAsync { throw AssertionError("Should never be called") }
        } else send(record, callback)
      })

      shouldThrow<RuntimeException> {
        publish(publisherSettings(), { failingProducer }) {
          offer(record)
        }
      } shouldBe boom
    }
  }

  "An async failure is rethrow at the end" {
    withTopic(partitions = 4) { topic ->
      val count = 3
      val boom = RuntimeException("Boom!")
      val records = (0..count).map {
        topic.createProducerRecord(it)
      }
      shouldThrow<RuntimeException> {
        publish(publisherSettings()) {
          publish(records)
          launch { throw boom }
        }
      } shouldBe boom

      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .map { record ->
          Pair(record.partition(), listOf(record.value()))
            .also { record.offset.acknowledge() }
        }
        .take(3 + 1)
        .toList()
        .toMap() shouldBe records.groupBy({ it.partition() }) { it.value() }
    }
  }

  "A failure of a sendAwait can be caught in the block" {
    withTopic(partitions = 4) { topic ->
      val record = topic.createProducerRecord(0)
      val record2 = topic.createProducerRecord(1)
      val failingProducer = producer.stubProducer(_sendCallback = { metadata, callback ->
        if (metadata.key().equals("0")) {
          callback.onCompletion(null, RuntimeException("Boom!"))
          CompletableFuture.supplyAsync { throw AssertionError("Should never be called") }
        } else send(record, callback)
      })

      publish(publisherSettings(), { failingProducer }) {
        publishCatching(record)
        offer(record2)
      }

      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .map {
          it.apply { offset.acknowledge() }
        }.first().value() shouldBe record.value()
    }
  }

  "concurrent publishing" {
    withTopic(partitions = 4) { topic ->
      val count = 4
      val records =
        (1..count).map {
          (it + 1..it + count).map { topic.createProducerRecord(it) }
        }

      publish(publisherSettings()) {
        listOf(
          async { offer(records[0]) },
          async { offer(records[1]) },
          async { publish(records[2]) },
          async { publish(records[3]) }
        ).awaitAll()
      }

      val expected =
        records.flatten().groupBy({ it.partition() }) { it.value() }.mapValues { it.value.toSet() }

      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .map { record ->
          record.also { record.offset.acknowledge() }
        }
        .take(records.flatten().size)
        .toList()
        .groupBy({ it.partition() }) { it.value() }
        .mapValues { it.value.toSet() } shouldBe expected
    }
  }

  "transaction an receive all messages that were published on the right partitions" {
    withTopic(partitions = 4) { topic ->
      val count = 3
      val records = (0..count).map {
        topic.createProducerRecord(it)
      }
      val settings = publisherSettings(
        acknowledgments = Acks.All,
        properties = Properties().apply {
          put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, testCase.name.testName)
          put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        }
      )
      publish(settings) {
        transaction {
          offer(records)
        }
      }

      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .map { record ->
          Pair(record.partition(), listOf(record.value()))
            .also { record.offset.acknowledge() }
        }
        .take(count + 1)
        .toList()
        .toMap() shouldBe records.groupBy({ it.partition() }) { it.value() }
    }
  }

  "A failure in a transaction aborts the transaction" {
    withTopic(partitions = 4) { topic ->
      val count = 3
      val boom = RuntimeException("Boom!")
      val records = (0..count).map {
        topic.createProducerRecord(it)
      }
      val settings = publisherSettings(
        acknowledgments = Acks.All,
        properties = Properties().apply {
          put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, testCase.name.testName)
          put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        }
      )
      shouldThrow<RuntimeException> {
        publish(settings) {
          transaction {
            publish(records)
            throw boom
          }
        }
      } shouldBe boom

      shouldTimeout(1.seconds) {
        KafkaReceiver(receiverSetting())
          .receive(topic.name())
          .take(1)
          .toList()
      }
    }
  }

  "An async failure in a transaction aborts the transaction" {
    withTopic(partitions = 4) { topic ->
      val count = 3
      val boom = RuntimeException("Boom!")
      val records = (0..count).map {
        topic.createProducerRecord(it)
      }
      val settings = publisherSettings(
        acknowledgments = Acks.All,
        properties = Properties().apply {
          put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, testCase.name.testName)
          put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        }
      )
      shouldThrow<RuntimeException> {
        publish(settings) {
          transaction {
            offer(records)
            launch { throw boom }
          }
        }
      } shouldBe boom

      shouldTimeout(1.seconds) {
        KafkaReceiver(receiverSetting())
          .receive(topic.name())
          .take(1)
          .toList()
      }
    }
  }

  "transaction - concurrent publishing" {
    withTopic(partitions = 4) { topic ->
      val count = 4
      val records =
        (1..count).map {
          (it + 1..it + count).map { topic.createProducerRecord(it) }
        }

      val settings = publisherSettings(
        acknowledgments = Acks.All,
        properties = Properties().apply {
          put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, testCase.name.testName)
          put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        }
      )

      publish(settings) {
        transaction {
          listOf(
            async { offer(records[0]) },
            async { offer(records[1]) },
            async { publish(records[2]) },
            async { publish(records[3]) }
          ).awaitAll()
        }
      }

      val expected =
        records.flatten().groupBy({ it.partition() }) { it.value() }.mapValues { it.value.toSet() }

      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .map { record ->
          record.also { record.offset.acknowledge() }
        }
        .take(records.flatten().size)
        .toList()
        .groupBy({ it.partition() }) { it.value() }
        .mapValues { it.value.toSet() } shouldBe expected
    }
  }
})

fun NewTopic.createProducerRecord(index: Int, partitions: Int = 4): ProducerRecord<String, String> {
  val partition: Int = index % partitions
  return ProducerRecord<String, String>(name(), partition, "$index", "Message $index")
}

fun <Key, Value> Producer<Key, Value>.stubProducer(
  _sendCallback: Producer<Key, Value>.(record: ProducerRecord<Key, Value>, callback: Callback) -> Future<RecordMetadata> =
    { record, callback -> send(record, callback) },
  _send: Producer<Key, Value>.(record: ProducerRecord<Key, Value>) -> Future<RecordMetadata> = { send(it) }
) = object : Producer<Key, Value> {
  override fun close() {}

  override fun close(timeout: Duration?) {}

  override fun initTransactions() =
    this@stubProducer.initTransactions()

  override fun beginTransaction() =
    this@stubProducer.beginTransaction()

  @Suppress("DEPRECATION")
  @Deprecated("Deprecated in Java")
  override fun sendOffsetsToTransaction(
    offsets: MutableMap<TopicPartition, OffsetAndMetadata>?,
    consumerGroupId: String?
  ) = this@stubProducer.sendOffsetsToTransaction(offsets, consumerGroupId)

  override fun sendOffsetsToTransaction(
    offsets: MutableMap<TopicPartition, OffsetAndMetadata>?,
    groupMetadata: ConsumerGroupMetadata?
  ) = this@stubProducer.sendOffsetsToTransaction(offsets, groupMetadata)

  override fun commitTransaction() =
    this@stubProducer.commitTransaction()

  override fun abortTransaction() =
    this@stubProducer.abortTransaction()

  override fun flush() =
    this@stubProducer.flush()

  override fun partitionsFor(topic: String?): MutableList<PartitionInfo> =
    this@stubProducer.partitionsFor(topic)

  override fun metrics(): MutableMap<MetricName, out Metric> =
    this@stubProducer.metrics()

  override fun send(record: ProducerRecord<Key, Value>, callback: Callback): Future<RecordMetadata> =
    _sendCallback.invoke(this@stubProducer, record, callback)

  override fun send(record: ProducerRecord<Key, Value>): Future<RecordMetadata> =
    _send.invoke(this@stubProducer, record)
}
