package io.github.nomisrev.kafka.publisher

import io.github.nomisRev.kafka.publisher.Acks
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisrev.kafka.KafkaSpec
import io.github.nomisrev.kafka.assertThrows
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart.UNDISPATCHED
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.ProducerFencedException
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class KafkaPublisherSpec : KafkaSpec() {

  @Test
  fun `All offered messages are received`() = withTopic {
    val count = 3
    val records = (0..count).map {
      createProducerRecord(it)
    }
    publishScope {
      offer(records)
    }

    topic.assertHasRecords(records)
  }

  @Test
  fun `Can receive all messages that were published on the right partitions`() = withTopic {
    val count = 3
    val records = (0..count).map {
      createProducerRecord(it)
    }
    publishScope {
      publish(records)
    }

    topic.assertHasRecords(records)
  }

  @Test
  fun `A failure in a produce block, rethrows the error`() = withTopic {
    val record = createProducerRecord(0)

    val exception = assertThrows<RuntimeException> {
      publishScope<Unit> {
        offer(record)
        throw Boom
      }
    }

    assertEquals(exception, Boom)
    topic.assertHasRecord(record)
  }

  @Test
  fun `A failure in a produce block with a concurrent launch cancels the launch, rethrows the error`() = withTopic {
    val cancelSignal = CompletableDeferred<CancellationException>()
    val exception = assertThrows<RuntimeException> {
      publishScope<Unit> {
        launch(start = UNDISPATCHED) {
          try {
            awaitCancellation()
          } catch (e: CancellationException) {
            cancelSignal.complete(e)
            throw e
          }
        }
        throw Boom
      }
    }

    assertEquals(exception, Boom)
    cancelSignal.await()
  }

  @Test
  fun `A failed offer is rethrown at the end`() = withTopic {
    val record = createProducerRecord(0)

    val exception = assertThrows<RuntimeException> {
      KafkaPublisher(publisherSettings(), stubProducer(failOnNumber = 0)).use {
        it.publishScope {
          offer(record)
        }
      }
    }

    assertEquals(exception, Boom)
  }

  @Test
  fun `An async failure is rethrow at the end`() = withTopic {
    val count = 3
    val records: List<ProducerRecord<String, String>> = (0..count)
      .map(::createProducerRecord)

    val exception = assertThrows<RuntimeException> {
      publishScope {
        publish(records)
        launch { throw Boom }
      }
    }

    assertEquals(exception, Boom)
    topic.assertHasRecords(records)
  }

  @Test
  fun `A failure of a sendAwait can be caught in the block`() = withTopic {
    val record0 = createProducerRecord(0)
    val record1 = createProducerRecord(1)

    KafkaPublisher(publisherSettings(), stubProducer(failOnNumber = 0)).use {
      it.publishScope {
        publishCatching(record0)
        offer(record1)
      }
    }

    topic.assertHasRecord(record1)
  }

  @Test
  fun `concurrent publishing`() = withTopic {
    val count = 4
    val records =
      (0..<count).map { base ->
        (base..base + count).map(::createProducerRecord)
      }

    publishScope {
      listOf(
        async { offer(records[0]) },
        async { offer(records[1]) },
        async { publish(records[2]) },
        async { publish(records[3]) }
      ).awaitAll()
    }

    topic.assertHasRecords(records)
  }

  @Test
  fun `transaction an receive all messages that were published on the right partitions`() = withTopic {
    val count = 3
    val records = (0..count).map(::createProducerRecord)
    val settings = publisherSettings(Acks.All) {
      put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction an receive all messages")
      put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    }

    KafkaPublisher(settings).use {
      it.publishScope {
        transaction {
          offer(records)
        }
      }
    }

    topic.assertHasRecords(records)
  }

  @Test
  fun `A failure in a transaction aborts the transaction`() = withTopic {
    val count = 3
    val records = (0..count).map(::createProducerRecord)
    val settings = publisherSettings(Acks.All) {
      put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "A failure in a transaction aborts")
      put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    }
    val exception = assertThrows<RuntimeException> {
      KafkaPublisher(settings).use {
        it.publishScope<Unit> {
          transaction {
            publish(records)
            throw Boom
          }
        }
      }
    }

    assertEquals(exception, Boom)
    topic.shouldBeEmpty()
  }

  @Test
  fun `An async failure in a transaction aborts the transaction`() = withTopic {
    val count = 3
    val records = (0..count).map(::createProducerRecord)
    val settings = publisherSettings(Acks.All) {
      put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "An async failure in a transaction aborts")
      put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    }
    val exception = assertThrows<RuntimeException> {
      KafkaPublisher(settings).use {
        it.publishScope {
          transaction {
            offer(records)
            launch { throw Boom }
          }
        }
      }
    }

    assertEquals(exception, Boom)
    topic.shouldBeEmpty()
  }

  @Test
  fun `transaction - concurrent publishing`() = withTopic {
    val count = 4
    val records =
      (0..<count).map { base ->
        (base..base + count).map(::createProducerRecord)
      }

    val settings = publisherSettings(Acks.All) {
      put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction - concurrent publishing")
      put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    }

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

    topic.assertHasRecords(records)
  }

  @Test
  fun `Only one KafkaProducer can have transactional_id at once, ProducerFencedException is fatal`() = withTopic {
    val settings = publisherSettings(Acks.All) {
      put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "Only one KafkaProducer can have transactional.id")
      put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    }
    val records1 = (0..4).map(::createProducerRecord)
    val publisher1 = KafkaPublisher(settings)
    publisher1.publishScope {
      transaction {
        publish(records1)
      }
    }

    val records2 = (5..9).map(::createProducerRecord)
    val publisher2 = KafkaPublisher(settings)
    publisher2.publishScope {
      transaction {
        publish(records2)
      }
    }

    // publisher1 was previous transactional.id, will result in fatal ProducerFencedException
    val records3 = (10..14).map(::createProducerRecord)
    assertThrows<ProducerFencedException> {
      publisher1.publishScope {
        transaction {
          publishCatching(records3)
        }
      }
    }

    // Due to ProducerFencedException, only records1 and records2 are received
    topic.assertHasRecords(records1 + records2)
  }

  @Test
  fun `idempotent publisher`() = withTopic {
    val records = (0..10).map(::createProducerRecord)
    launch(start = UNDISPATCHED) {
      KafkaPublisher(publisherSettings {
//          put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
      }).use {
        it.publishScope {
          records.forEach { r ->
            offer(r)
            delay(100)
          }
        }
      }
    }

    kafka.pause()

    delay(2000)

    kafka.unpause()

    topic.assertHasRecords(records)
  }
}
