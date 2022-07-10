package io.github.nomisRev.kafka.custom

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration

internal object Logger {
  internal fun debug(msg: String): Unit = println("DEBUG: $msg")
  internal fun trace(msg: String): Unit = println("TRACE: $msg")
  internal fun error(msg: String, throwable: Throwable? = null): Unit {
    println("ERROR: $msg")
    throwable?.printStackTrace()
  }
  internal fun warn(msg: String, throwable: Throwable? = null): Unit {
    println("WARN: $msg")
    throwable?.printStackTrace()
  }
  
  fun isTraceEnabled(): Boolean = true
  fun isDebugEnabled(): Boolean = true
}

internal val log: Logger = Logger

// internal fun <K, V> KafkaConsumer<K, V>.subscriber(
//   topicNames: Collection<String>,
//   pollEvent: PollEvent<K, V>
// ): Unit =
//   subscribe(topicNames, object : ConsumerRebalanceListener {
//     override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
//       log.debug("onPartitionsAssigned $partitions")
//       // onAssign methods may perform seek. It is safe to use the consumer here since we are in a poll()
//       if (partitions.isNotEmpty()) {
//         if (pollEvent.pausedByUs.get()) {
//           log.debug("Rebalance during back pressure, re-pausing new assignments")
//           pause(partitions)
//         }
//         // for (onAssign in receiverOptions.assignListeners()) {
//         //   onAssign.accept(toSeekable(partitions))
//         // }
//         if (log.isTraceEnabled()) {
//           try {
//             val positions = partitions.map { part: TopicPartition? ->
//               "$part pos: ${position(part, Duration.ofSeconds(5))}"
//             }
//             log.trace("positions: $positions, committed: ${committed(HashSet(partitions), Duration.ofSeconds(5))}")
//           } catch (ex: Exception) {
//             log.error("Failed to get positions or committed", ex)
//           }
//         }
//       }
//     }
//
//     override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
//       log.debug("onPartitionsRevoked $partitions")
//       // pollEvent.commitBatch.partitionsRevoked(partitions)
//       // this@ConsumerEventLoop.onPartitionsRevoked(partitions)
//     }
//   })
