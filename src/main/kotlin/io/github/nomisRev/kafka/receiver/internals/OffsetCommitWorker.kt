package io.github.nomisRev.kafka.receiver.internals

import io.github.nomisRev.kafka.receiver.CommitStrategy
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.channels.onSuccess
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.whileSelect

/**
 * A suspend function that will schedule [commit] based on the [AckMode], [CommitStrategy] and [commitSignal].
 * This function should be run in a parallel-process alongside the [EventLoop],
 * this way we have a separate process managing and scheduling the commits.
 *
 * KotlinX Coroutines exposes a powerful experimental API where we can listen to our [commitSignal],
 * while racing against a [onTimeout]. This allows for easily committing on whichever event arrives first.
 *
 * If you send a [Unit] to the [commitSignal] it will commit immediately,
 * if you send a [null] it will reset the timer
 */
@OptIn(ExperimentalCoroutinesApi::class)
internal suspend fun offsetCommitWorker(
  ackMode: AckMode,
  strategy: CommitStrategy,
  commitSignal: Channel<Unit>,
  commit: suspend () -> Unit,
): Unit = if (ackMode == AckMode.MANUAL_ACK || ackMode == AckMode.AUTO_ACK) {
  whileSelect {
    when (strategy) {
      is CommitStrategy.BySizeOrTime -> {
        commitSignal.onReceiveCatching {
          commit()
          !it.isClosed
        }
        onTimeout(strategy.interval) {
          commit()
          true
        }
      }

      is CommitStrategy.BySize ->
        commitSignal.onReceiveCatching {
          commit()
          !it.isClosed
        }

      is CommitStrategy.ByTime ->
        onTimeout(strategy.interval) {
          commit()
          true
        }
    }
  }
} else Unit
