package io.github.nomisRev.kafka.publisher

/**
 * The number of acknowledgments the producer requires the leader to have received before considering a request complete.
 * This controls the durability of records that are sent
 *
 * **Note:** that enabling idempotence requires this config value to be 'all'.
 * If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled.
 */
public enum class Acks(public val value: String) {
  /**
   * If set to zero then the producer will not wait for any acknowledgment from the server at all.
   * The record will be immediately added to the socket buffer and considered sent.
   * No guarantee can be made that the server has received the record in this case,
   * and the <code>retries</code> configuration will not take effect (as the client won't generally know of any failures).
   * The offset given back for each record will always be set to `-1`
   */
  Zero("0"),

  /**
   * This will mean the leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers.
   * In this case should the leader fail immediately after acknowledging the record but before the followers have replicated it then the record will be lost.
   */
  One("1"),

  /**
   * This means the leader will wait for the full set of in-sync replicas to acknowledge the record.
   * This guarantees that the record will not be lost as long as at least one in-sync replica remains alive.
   * This is the strongest available guarantee. This is equivalent to the `acks=-1` setting.
   */
  All("all"),

  /**
   * Alias to all
   * @see All
   */
  MinusOne("-1"),
}
