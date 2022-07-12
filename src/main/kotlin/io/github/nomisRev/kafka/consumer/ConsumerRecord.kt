package io.github.nomisRev.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Represents an incoming record.
 *
 * @param <K> Incoming record key type
 * @param <V> Incoming record value type
 */
public class ReceiverRecord<K, V>(
  record: ConsumerRecord<K, V>,
  /**
   * Returns an acknowledgeable offset that should be acknowledged after this record has been consumed.
   * Acknowledged records are automatically committed based on the configured [CommitStrategy].
   * Acknowledged records may be also committed using [Offset.commit].
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