package io.github.nomisRev.kafka.consumer.internals

import io.github.nomisRev.kafka.consumer.CommitStrategy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.whileSelect

@OptIn(ExperimentalCoroutinesApi::class)
internal fun CoroutineScope.offsetCommitWorker(
  ackMode: AckMode,
  strategy: CommitStrategy,
  commitSignal: Channel<Unit>,
  commit: suspend () -> Unit,
): Job = if (ackMode == AckMode.MANUAL_ACK || ackMode == AckMode.AUTO_ACK) {
  launch {
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
  }
} else Job()
