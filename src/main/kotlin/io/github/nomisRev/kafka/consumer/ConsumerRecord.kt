package io.github.nomisRev.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Represents an incoming record dispatched by [KafkaReceiver].
 *
 * @param <K> Incoming record key type
 * @param <V> Incoming record value type
 */
public class ReceiverRecord<K, V>(
  record: ConsumerRecord<K, V>,
  /**
   * Returns an acknowledgeable offset instance that should be acknowledged after this
   * record has been consumed. Acknowledged records are automatically committed
   * based on the commit batch size and commit interval configured for the [KafkaReceiver].
   * Acknowledged records may be also committed using [Offset.commit].
   *
   * @return offset to acknowledge after record is processed
   */
  public val offset: Offset,
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