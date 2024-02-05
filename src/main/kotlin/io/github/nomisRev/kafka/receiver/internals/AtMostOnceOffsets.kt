package io.github.nomisRev.kafka.receiver.internals

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.ConcurrentHashMap

internal class UtmostOnceOffsets {
  private val committedOffsets = ConcurrentHashMap<TopicPartition, Long>()
  private val dispatchedOffsets = ConcurrentHashMap<TopicPartition, Long>()
  
  fun onCommit(offsets: Map<TopicPartition, OffsetAndMetadata>) =
    offsets.forEach { (key, value) ->
      committedOffsets[key] = value.offset()
    }
  
  fun onDispatch(topicPartition: TopicPartition, offset: Long) {
    dispatchedOffsets[topicPartition] = offset
  }
  
  fun committedOffset(topicPartition: TopicPartition): Long =
    committedOffsets[topicPartition] ?: -1
  
  /*suspend*/ fun undoCommitAhead(committableBatch: CommittableBatch): Boolean {
    var undoRequired = false
    committedOffsets.forEach { (topicPartition, value) ->
      // TODO this should be safe. Add requireNotNull with better error message
      val offsetToCommit = dispatchedOffsets[topicPartition]!! + 1
      if (value > offsetToCommit) {
        committableBatch.updateOffset(topicPartition, offsetToCommit)
        undoRequired = true
      }
    }
    return undoRequired
  }
}
