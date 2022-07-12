package io.github.nomisRev.kafka.consumer

import kotlin.time.Duration

/**
 * The strategy to apply to the "Offset commit manager".
 * Offsets can be committed to kafka using different strategies:
 *
 *  - Every `n` acknowledged records
 *  - Every `interval` duration, commit *all* acknowledged records
 *  - Every `n` acknowledged records **or** every `interval` duration, whichever comes first.
 */
public sealed interface CommitStrategy {
  /** Commit *all* [Offset.acknowledge] messages to kafka every [interval]. */
  @JvmInline
  public value class ByTime(public val interval: Duration) : CommitStrategy
  
  /** Commit messages to kafka every [size] acknowledged message. */
  @JvmInline
  public value class BySize(public val size: Int) : CommitStrategy
  
  /**
   * Commit messages to kafka every [size] acknowledged message, or every [interval].
   * Whichever condition is reached first.
   */
  public data class BySizeOrTime(public val size: Int, public val interval: Duration) : CommitStrategy
}
