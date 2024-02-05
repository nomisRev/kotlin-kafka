package io.github.nomisrev.kafka.publisher

import io.github.nomisRev.kafka.publisher.produce
import io.github.nomisRev.kafka.publisher.produceOrThrow
import io.github.nomisrev.kafka.KafkaSpec
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
import org.apache.kafka.clients.producer.RecordMetadata
import org.junit.jupiter.api.Test
import kotlin.test.Ignore
import kotlin.test.assertEquals

class FlowProduceSpec : KafkaSpec() {
  @Test
  fun `produce - All offered messages are received`() = withTopic {
    val count = 10_000
    val records = produce(count)
      .asFlow()
      .flowOn(Dispatchers.IO)

    records
      .produce(publisherSettings())
      .flowOn(Dispatchers.Default)
      .collect()

    topic.assertHasRecords(records.toList())
  }

  @Test
  fun `produce - Failure does not stop previous send messages`() = withTopic {
    var count = 0
    val records = produce(10)
    val flow = records
      .asFlow()
      .flowOn(Dispatchers.IO)
      .append { throw Boom }

    val e = runCatching {
      flow
        .produce(publisherSettings())
        .flowOn(Dispatchers.Default)
        .onEach { count++ }
        .collect()
    }.exceptionOrNull()

    assertEquals(records.size, count)
    assertEquals(Boom, e)
    topic.assertHasRecords(records)
  }

  @Test
  fun `produce - Can handle exception from upstream`() = withTopic {
    val records = produce(0..4)
    val records2 = produce(5..10)
    val allRecords = records + records2

    val flow = records
      .asFlow()
      .flowOn(Dispatchers.IO)
      .append { throw Boom }
      .catch { emitAll(records2) }

    val count = flow
      .produce(publisherSettings())
      .flowOn(Dispatchers.Default)
      .toList()

    assertEquals(allRecords.size, count.size)
    topic.assertHasRecords(allRecords)
  }

  @Test
  fun `produce - Can handle exception from producer`() = withTopic {
    val buffer = mutableListOf<Result<RecordMetadata>>()
    val records = produce(10)

    val flow = records
      .asFlow()
      .flowOn(Dispatchers.IO)

    val result = runCatching {
      flow
        .produce(publisherSettings().copy(createProducer = stubProducer(failOnNumber = 5)))
        .flowOn(Dispatchers.Default)
        .onEach { buffer.add(it) }
        .collect()
    }

    assertEquals(records.size, buffer.size)
    assertEquals(buffer.count { it.isFailure }, 1)
    assertEquals(result.exceptionOrNull(), Boom)
    topic.assertHasRecords(records.toMutableList().apply { removeAt(5) })
  }

  @Test
  fun `produce - Can catch exception from producer`() = withTopic {
    val error = CompletableDeferred<Throwable>()
    val records = produce(10)

    val flow = records
      .asFlow()
      .flowOn(Dispatchers.IO)

    flow
      .produce(publisherSettings().copy(createProducer = stubProducer(failOnNumber = 5)))
      .catch { require(error.complete(it)) { "Only 1 error expected." } }
      .flowOn(Dispatchers.Default)
      .toList()

    assertEquals(Boom, error.await())
    topic.assertHasRecords(records.toMutableList().apply { removeAt(5) })
  }

  @Test
  fun `produceOrThrow - All offered messages are received`() = withTopic {
    val records = produce(10)
      .asFlow()
      .flowOn(Dispatchers.IO)

    records
      .produceOrThrow(publisherSettings())
      .flowOn(Dispatchers.Default)
      .collect()

    topic.assertHasRecords(records.toList())
  }

  @Test
  fun `produceOrThrow - Failure does not stop previous send messages`() = withTopic {
    val records = produce(10)
    val flow = records
      .asFlow()
      .flowOn(Dispatchers.IO)
      .append { throw Boom }

    val result = runCatching {
      flow
        .produceOrThrow(publisherSettings())
        .flowOn(Dispatchers.Default)
        .collect()
    }

    assertEquals(Boom, result.exceptionOrNull())
    topic.assertHasRecords(records)
  }

  @Test
  fun `produceOrThrow - Can catch exception from producer`() = withTopic {
    val error = CompletableDeferred<Throwable>()
    val records = produce(10)

    val flow = records
      .asFlow()
      .flowOn(Dispatchers.IO)

    flow
      .produceOrThrow(publisherSettings().copy(createProducer = stubProducer(failOnNumber = 5)))
      .catch { require(error.complete(it)) { "Only 1 error expected." } }
      .flowOn(Dispatchers.Default)
      .toList()

    assertEquals(Boom, error.await())
    topic.assertHasRecords(records.toMutableList().apply { removeAt(5) })
  }

  @Test
  @Ignore
  fun stressSlow() = withTopic {
    val records = produce(10)

    val flow = records.asFlow()
      .onEach { delay(25_000) }
      .flowOn(Dispatchers.IO)

    flow
      .produce(publisherSettings())
      .flowOn(Dispatchers.Default)
      .collect()

    topic.assertHasRecords(records)
  }
}

private suspend fun <T> FlowCollector<T>.emitAll(iterable: Iterable<T>) =
  iterable.forEach { emit(it) }

private fun <A> Flow<A>.append(block: suspend FlowCollector<A>.() -> Unit): Flow<A> =
  flow {
    collect { emit(it) }
    block()
  }
