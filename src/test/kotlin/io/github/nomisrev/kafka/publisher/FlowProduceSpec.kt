package io.github.nomisrev.kafka.publisher

import io.github.nomisRev.kafka.publisher.produce
import io.github.nomisrev.kafka.KafkaSpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class FlowProduceSpec : KafkaSpec() {
  @Test
  fun `All offered messages are received`() = withTopic {
    val count = 3
    val records = (0..count)
      .asFlow()
      .map { createProducerRecord(it) }
      .flowOn(Dispatchers.IO)

    records
      .produce(publisherSettings)
      .flowOn(Dispatchers.Default)
      .collect()

    topic.assertHasRecords(records.toList())
  }

  @Test
  fun `Failure does not stop producing messages`() = withTopic {
    val count = 10
    val error = RuntimeException("boom")
    val records =
      (0..4).map { createProducerRecord(it) }
        .asFlow()
        .append { throw error }
        .append { emitAll((6..count).map { createProducerRecord(it) }) }

    val e = runCatching {
      records
        .produce(publisherSettings)
        .flowOn(Dispatchers.Default)
        .collect()
    }.exceptionOrNull()
    assertEquals(e?.message, error.message)

    topic.assertHasRecords((0..4).map { createProducerRecord(it) })
  }

  @Test
  fun `Can handle exception from produce`() = withTopic {
    val count = 10
    val error = RuntimeException("boom")
    val records =
      flow {
        (0..count).forEach { emit(createProducerRecord(it)) }
        throw error
      }.flowOn(Dispatchers.IO)

    val e = runCatching {
      records
        .produce(publisherSettings)
        .catch { e -> if (e.message == error.message) Unit else throw e }
        .flowOn(Dispatchers.Default)
        .collect()
    }.exceptionOrNull()
    assertEquals(e, null)

    topic.assertHasRecords((0..count).map { createProducerRecord(it) })
  }
}

fun <A> Flow<A>.append(block: suspend FlowCollector<A>.() -> Unit): Flow<A> =
  flow {
    this@append.collect { emit(it) }
    block()
  }

suspend fun <A> FlowCollector<A>.emitAll(iterable: Iterable<A>): Unit =
  iterable.forEach { emit(it) }