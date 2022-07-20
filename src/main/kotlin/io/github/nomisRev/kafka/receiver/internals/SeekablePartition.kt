package io.github.nomisRev.kafka.receiver.internals

import io.github.nomisRev.kafka.receiver.ConsumerPartition
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import java.util.Collections

// This implementation offers an API with the TopicPartition partially applied.
internal class SeekablePartition(
  private val consumer: Consumer<*, *>,
  override val topicPartition: TopicPartition,
) : ConsumerPartition {
  
  override fun seekToBeginning(): Unit =
    consumer.seekToBeginning(listOf(topicPartition))
  
  override fun seekToEnd(): Unit =
    consumer.seekToEnd(listOf(topicPartition))
  
  override fun seek(offset: Long): Unit =
    consumer.seek(topicPartition, offset)
  
  override fun seekToTimestamp(timestamp: Long) {
    val offsets = consumer.offsetsForTimes(Collections.singletonMap(topicPartition, timestamp))
    val next = offsets.values.iterator().next()
    if (next == null) seekToEnd()
    else consumer.seek(topicPartition, next.offset())
  }
  
  override fun position(): Long =
    consumer.position(topicPartition)
  
  override fun toString(): String =
    topicPartition.toString()
}
