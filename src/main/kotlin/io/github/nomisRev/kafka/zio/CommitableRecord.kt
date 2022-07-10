package io.github.nomisRev.kafka.zio

import org.apache.kafka.clients.consumer.ConsumerRecord

public data class CommittableRecord<K, V>(
  val record: ConsumerRecord<K, V>,
  val offset: Offset,
) : ConsumerRecord<K, V>(
  record.topic(),
  record.partition(),
  record.offset(),
  record.timestamp(),
  record.timestampType(),
  record.serializedKeySize(),
  record.serializedValueSize(),
  record.key(),
  record.value(),
  record.headers(),
  record.leaderEpoch()
)
