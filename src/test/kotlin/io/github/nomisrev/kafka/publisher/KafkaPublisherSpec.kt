package io.github.nomisrev.kafka.publisher

import io.github.nomisRev.kafka.publisher.Acks
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisrev.kafka.KafkaSpec
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
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.ProducerFencedException
import java.util.Properties

class KafkaPublisherSpec : KafkaSpec({

  "Can receive all messages that were offered on the right partitions" {
    withTopic(partitions = 4) { topic ->
      val count = 3
      val records = (0..count).map {
        topic.createProducerRecord(it)
      }
      publisher.publishScope {
        offer(records)
      }

      topic.shouldHaveRecords(records)
    }
  }

  "Can receive all messages that were published on the right partitions" {
    withTopic(partitions = 4) { topic ->
      val count = 3
      val records = (0..count).map {
        topic.createProducerRecord(it)
      }
      publisher.publishScope {
        publish(records)
      }

      topic.shouldHaveRecords(records)
    }
  }

  "A failure in a produce block, rethrows the error" {
    withTopic(partitions = 4) { topic ->
      val record = topic.createProducerRecord(0)

      shouldThrow<RuntimeException> {
        publisher.publishScope {
          offer(record)
          throw boom
        }
      } shouldBe boom

      topic.shouldHaveRecord(record)
    }
  }

  "A failure in a produce block with a concurrent launch cancels the launch, rethrows the error" {
    withTopic(partitions = 4) { topic ->
      val cancelSignal = CompletableDeferred<CancellationException>()
      shouldThrow<RuntimeException> {
        publisher.publishScope {
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

  "A failed offer is rethrown at the end" {
    withTopic(partitions = 4) { topic ->
      val record = topic.createProducerRecord(0)

      shouldThrow<RuntimeException> {
        KafkaPublisher(publisherSettings(), stubProducer(failOnNumber = 0)).publishScope {
          offer(record)
        }
      } shouldBe boom
    }
  }

  "A failed offer await is rethrow at the end" {
    withTopic(partitions = 4) { topic ->
      val record0 = topic.createProducerRecord(0)
      val record1 = topic.createProducerRecord(1)
      shouldThrow<RuntimeException> {
        KafkaPublisher(publisherSettings(), stubProducer(failOnNumber = 1)).publishScope {
          publish(record0)
          offer(record1).await()
        }
      } shouldBe boom

      topic.shouldHaveRecord(record0)
    }
  }

  "An async failure is rethrow at the end" {
    withTopic(partitions = 4) { topic ->
      val count = 3
      val records: List<ProducerRecord<String, String>> = (0..count).map {
        topic.createProducerRecord(it)
      }
      shouldThrow<RuntimeException> {
        publisher.publishScope {
          publish(records)
          launch { throw boom }
        }
      } shouldBe boom

      topic.shouldHaveRecords(records)
    }
  }

  "A failure of a sendAwait can be caught in the block" {
    withTopic(partitions = 4) { topic ->
      val record0 = topic.createProducerRecord(0)
      val record1 = topic.createProducerRecord(1)

      KafkaPublisher(publisherSettings(), stubProducer(failOnNumber = 0)).use {
        it.publishScope {
          publishCatching(record0)
          offer(record1)
        }
      }

      topic.shouldHaveRecord(record1)
    }
  }

  "concurrent publishing" {
    withTopic(partitions = 4) { topic ->
      val count = 4
      val records =
        (1..count).map {
          (it + 1..it + count).map { topic.createProducerRecord(it) }
        }

      publisher.publishScope {
        listOf(
          async { offer(records[0]) },
          async { offer(records[1]) },
          async { publish(records[2]) },
          async { publish(records[3]) }
        ).awaitAll()
      }

      topic.shouldHaveRecords(records)
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
      KafkaPublisher(settings).use {
        it.publishScope {
          transaction {
            offer(records)
          }
        }
      }

      topic.shouldHaveRecords(records)
    }
  }

  "A failure in a transaction aborts the transaction" {
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
      shouldThrow<RuntimeException> {
        KafkaPublisher(settings).use {
          it.publishScope {
            transaction {
              publish(records)
              throw boom
            }
          }
        }
      } shouldBe boom

      topic.shouldBeEmpty()
    }
  }

  "An async failure in a transaction aborts the transaction" {
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
      shouldThrow<RuntimeException> {
        KafkaPublisher(settings).use {
          it.publishScope {
            transaction {
              offer(records)
              launch { throw boom }
            }
          }
        }
      } shouldBe boom

      topic.shouldBeEmpty()
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

      KafkaPublisher(settings).use {
        it.publishScope {
          transaction {
            listOf(
              async { offer(records[0]) },
              async { offer(records[1]) },
              async { publish(records[2]) },
              async { publish(records[3]) }
            ).awaitAll()
          }
        }
      }

      topic.shouldHaveRecords(records)
    }
  }

  "Only one transactional sender with a specific transactional id is allowed at the same time" {
    withTopic(partitions = 4) { topic ->
      val settings = publisherSettings(
        acknowledgments = Acks.All,
        properties = Properties().apply {
          put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, testCase.name.testName)
          put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        }
      )
      val records1 = (0..4).map { topic.createProducerRecord(it) }
      val publisher1 = KafkaPublisher(settings)
      publisher1.publishScope {
        transaction {
          publish(records1)
        }
      }

      val records2 = (5..9).map { topic.createProducerRecord(it) }
      val publisher2 = KafkaPublisher(settings)
      publisher2.publishScope {
        transaction {
          publish(records2)
        }
      }

      val records3 = (10..14).map { topic.createProducerRecord(it) }
      shouldThrow<ProducerFencedException> {
        publisher1.publishScope {
          transaction {
            // Test that ProducerFencedException is fatal
            publishCatching(records3)
          }
        }
      }

      topic.shouldHaveRecords(records1 + records2)
    }
  }
})

fun NewTopic.createProducerRecord(index: Int, partitions: Int = 4): ProducerRecord<String, String> {
  val partition: Int = index % partitions
  return ProducerRecord<String, String>(name(), partition, "$index", "Message $index")
}
