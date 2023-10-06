package io.github.nomisRev.kafka.receiver

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

  /** Commit **all** [Offset.acknowledge] messages to kafka every [interval]. */
  @JvmInline
  public value class ByTime(public val interval: Duration) : CommitStrategy {
    init {
      require(interval.isPosNonZero()) {
        "Time based auto-commit requires positive non-zero interval but found $interval"
      }
    }
  }

  /** Commit messages to kafka every [size] acknowledged message. */
  @JvmInline
  public value class BySize(public val size: Int) : CommitStrategy {
    init {
      require(size > 0) {
        "Size based auto-commit requires positive non-zero commit batch size but found $size"
      }
    }
  }

  /**
   * Commit messages to kafka every [size] acknowledged message, or every [interval].
   * Whichever condition is reached first.
   */
  public data class BySizeOrTime(public val size: Int, public val interval: Duration) : CommitStrategy {
    init {
      require(size > 0) {
        "Size based auto-commit requires positive non-zero commit batch size but found $size"
      }
      require(interval.isPosNonZero()) {
        "Time based auto-commit requires positive non-zero interval but found $interval"
      }
    }
  }
}

internal fun CommitStrategy.size(): Int =
  when (this) {
    is CommitStrategy.BySize -> size
    is CommitStrategy.BySizeOrTime -> size
    is CommitStrategy.ByTime -> 0
  }

internal fun Duration.isPosNonZero(): Boolean =
  this != Duration.ZERO && isPositive()
