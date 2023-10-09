package io.github.nomisRev.kafka.publisher

import org.apache.kafka.clients.producer.RecordMetadata

/**
 * An acknowledgment message  of a published record from Kafka.
 * This message was either a [Success] or [Failed] message,
 * and always contains the [correlationMetadata] from the [PublisherRecord] that was not sent to Kafka,
 * but enables matching this response to its corresponding request.
 *
 * The result returns when the record is acknowledged based on the [Acks] configuration.
 * If acks is not zero, sends are retried if [ProducerConfig.RETRIES_CONFIG] is configured.
 */
sealed interface RecordAck<A> {
  /**
   * Returns the correlation metadata associated with this instance to enable this
   * result to be matched with the corresponding [PublisherRecord] that was sent to Kafka.
   * @return correlation metadata
   */
  val correlationMetadata: A

  /**
   * Returns the exception associated with a send failure. This is set to null for
   * successful responses.
   * @return send exception from Kafka [Producer] if send did not succeed even after
   * the configured retry attempts.
   */
  data class Failed<A>(val exception: Exception, override val correlationMetadata: A) : RecordAck<A>

  /**
   * Returns the record metadata returned by Kafka. May be null if send request failed.
   * See [.exception] for failure reason when record metadata is null.
   * @return response metadata from Kafka [Producer]
   */
  data class Success<A>(val recordMetadata: RecordMetadata, override val correlationMetadata: A) : RecordAck<A>

  fun exceptionOrNull(): Exception? =
    when (this) {
      is Failed -> exception
      is Success -> null
    }

  fun recordMetadataOrNull(): RecordMetadata? =
    when (this) {
      is Failed -> null
      is Success -> recordMetadata
    }
}