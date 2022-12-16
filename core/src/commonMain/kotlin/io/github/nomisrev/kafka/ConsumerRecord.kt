package io.github.nomisrev.kafka

data class ConsumerRecord<K, V>(
  val topic: String,
  val partition: Int,
  val offset: Long,
  val timestamp: Long,
  val timestampType: TimestampType,
  val serializedKeySize: Int,
  val serializedValueSize: Int,
  val key: K?,
  val value: V?,
  val headers: Headers?,
  val leaderEpoch: Int?
)
