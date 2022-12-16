package io.github.nomisrev.kafka.receiver

import io.github.nomisrev.kafka.Headers
import io.github.nomisrev.kafka.TimestampType

/**
 * A [org.apache.kafka.clients.consumer.ConsumerRecord] for keys of [K], and values of [V].
 * With a property [Offset] which allows acknowledging, or committing record offsets to kafka.
 */
public class ReceiverRecord<K, V>(
  val topic: String,
  val partition: Int,
  val timestamp: Long,
  val timestampType: TimestampType,
  val serializedKeySize: Int,
  val serializedValueSize: Int,
  val key: K?,
  val value: V?,
  val headers: Headers?,
  val leaderEpoch: Int?,
  /**
   * Returns an acknowledgeable offset that should be acknowledged after this record has been consumed.
   * Acknowledged records are automatically committed based on the configured [CommitStrategy].
   * Acknowledged records may be also committed using [Offset.commit].
   */
  public val offset: Offset,
)
