package io.github.nomisRev.kafka.publisher

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header


/**
 * Represents an outgoing record. Along with the record to send to Kafka,
 * additional correlation metadata may also be specified to correlate
 * [PublisherRecord] to its corresponding record.
 *
 * @param <K> Outgoing record key type
 * @param <V> Outgoing record value type
 * @param <T> Correlation metadata type
 */
class PublisherRecord<K, V, T>
/**
 *  @param topic Topic to which record is sent
 *  @param partition The partition to which the record is sent. If null, the partitioner configured
 *  for the [KafkaSender] will be used to choose the partition.
 *  @param timestamp The timestamp of the record. If null, the current timestamp will be assigned by the producer.
 *  The timestamp will be overwritten by the broker if the topic is configured with
 *  [org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME]. The actual timestamp
 *  used will be returned in [SenderResult.recordMetadata]
 *  @param key The key to be included in the record. May be null.
 *  @param value The contents to be included in the record.
 *  @param correlationMetadata Additional correlation metadata that is not sent to Kafka, but is
 *  included in the response to match [SenderResult] to this record.
 *  @return new sender record that can be sent to Kafka using [KafkaSender.send]
 */
private constructor(
  topic: String,
  partition: Int?,
  timestamp: Long?,
  key: K,
  value: V,
  val correlationMetadata: T,
  headers: Iterable<Header>? = null
) : ProducerRecord<K, V>(topic, partition, timestamp, key, value, headers) {
  constructor(record: ProducerRecord<K, V>, correlationMetadata: T) :
    this(
      record.topic(),
      record.partition(),
      record.timestamp(),
      record.key(),
      record.value(),
      correlationMetadata,
      record.headers()
    )
}
