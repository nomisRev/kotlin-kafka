package io.github.nomisRev.kafka.receiver

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * A [org.apache.kafka.clients.consumer.ConsumerRecord] for keys of [K], and values of [V].
 * With a property [Offset] which allows acknowledging, or committing record offsets to kafka.
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