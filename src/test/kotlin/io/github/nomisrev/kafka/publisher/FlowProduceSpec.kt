package io.github.nomisrev.kafka.publisher

import io.github.nomisRev.kafka.publisher.produce
import io.github.nomisrev.kafka.KafkaSpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class FlowProduceSpec : KafkaSpec() {
  @Test
  fun `All offered messages are received`() = withTopic {
    val count = 10
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
    var count = 0
    val records = (0..10).map { createProducerRecord(it) }
    val flow = records
      .asFlow()
      .flowOn(Dispatchers.IO)
      .append { throw boom }

    val e = runCatching {
      flow
        .produce(publisherSettings)
        .flowOn(Dispatchers.Default)
        .onEach { count++ }
        .collect()
    }.exceptionOrNull()

    assertEquals(records.size, count)
    assertEquals("Boom!", e?.message)
    topic.assertHasRecords(records)
  }

  @Test
  fun `Can handle exception from produce`() = withTopic {
    val records = (0..10)
      .map { createProducerRecord(it) }

    val flow = records
      .asFlow()
      .flowOn(Dispatchers.IO)
      .append { throw boom }

    val count = flow
      .catch { }
      .produce(publisherSettings)
      .flowOn(Dispatchers.Default)
      .toList()

    assertEquals(records.size, count.size)
    topic.assertHasRecords(records)
  }
}

private fun <A> Flow<A>.append(block: suspend FlowCollector<A>.() -> Unit): Flow<A> =
  flow {
    collect { emit(it) }
    block()
  }
