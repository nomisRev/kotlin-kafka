package io.github.nomisRev.kafka.publisher

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow

interface SendOperator<Key, Value> : Flow<Unit> {
  fun <T> send(records: Flow<PublisherRecord<Key, Value, T>>): SendOperator<Key, Value>
}

private class DefaultSendOperator<Key, Value>(
  val wrapping: Flow<RecordAck<Any?>>,
  val publisher: DefaultKafkaPublisher<Key, Value>
) : SendOperator<Key, Value> {

  override suspend fun collect(collector: FlowCollector<Unit>): Unit =
    wrapping.collect {
      collector.emit(Unit)
    }

  override fun <T> send(records: Flow<PublisherRecord<Key, Value, T>>): SendOperator<Key, Value> =
    DefaultSendOperator(
      flow {
        wrapping.collect()
        publisher.send(records).collect()
      },
      publisher
    )
}
