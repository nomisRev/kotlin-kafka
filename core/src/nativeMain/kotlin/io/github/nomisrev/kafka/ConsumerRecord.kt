//package io.github.nomisrev.kafka
//
//
//value class ConsumerRecordWrapper<K, V>(
//  val delegate: com.icemachined.kafka.clients.consumer.ConsumerRecord<K, V>
//) : ConsumerRecord<K, V> {
//  override val topic: String
//    get() = delegate.topic
//  override val partition: Int
//    get() = delegate.partition
//  override val offset: Long
//    get() = delegate.offset.toLong()
//  override val timestamp: Long
//    get() = delegate.timestamp
//  override val timestampType: TimestampType
//    get() = delegate.timestampType
//  override val serializedKeySize: Int
//    get() = delegate.serializedKeySize
//  override val serializedValueSize: Int
//    get() = delegate.serializedValueSize
//  override val key: K?
//    get() = delegate.key
//  override val value: V?
//    get() = delegate.value
//  override val headers: Headers?
//    get() = delegate.headers
//  override val leaderEpoch: Int?
//    get() = delegate.leaderEpoch
//}