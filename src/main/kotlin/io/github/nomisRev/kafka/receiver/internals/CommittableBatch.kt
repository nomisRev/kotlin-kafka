package io.github.nomisRev.kafka.receiver.internals

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.LinkedList
import java.util.function.Consumer
import kotlin.coroutines.Continuation

private val logger: Logger =
  LoggerFactory.getLogger(CommittableBatch::class.java)

// TODO Replaced @Synchronized with Mutex & Atomic depending on usages
internal class CommittableBatch {
  var outOfOrderCommits = false
  val consumedOffsets: MutableMap<TopicPartition, Long> = HashMap()
  val uncommitted: MutableMap<TopicPartition, MutableList<Long>> = HashMap()
  val deferred: MutableMap<TopicPartition, MutableList<Long>> = HashMap()
  
  private val latestOffsets: MutableMap<TopicPartition, Long> = HashMap()
  private var batchSize = 0
  private var continuations: MutableList<Continuation<Unit>> = ArrayList()
  
  // Called from only from suspend code
  @Synchronized
  fun updateOffset(topicPartition: TopicPartition, offset: Long): Int {
    if (outOfOrderCommits) {
      val uncommittedThisTP: List<Long>? = uncommitted[topicPartition]
      if (uncommittedThisTP != null && uncommittedThisTP.contains(offset)) {
        val offsets = deferred.computeIfAbsent(
          topicPartition
        ) { _: TopicPartition? -> LinkedList() }
        if (!offsets.contains(offset)) {
          offsets.add(offset)
          batchSize++
        }
      } else {
        logger.debug("No uncommitted offset for $topicPartition@$offset, partition revoked?")
      }
    } else if (offset != consumedOffsets.put(topicPartition, offset)) {
      batchSize++
    }
    return batchSize
  }
  
  // Called from non-suspend code, atomic??
  @Synchronized
  fun addContinuation(continuation: Continuation<Unit>) =
    continuations.add(continuation)
  
  // called from suspend code
  @Synchronized
  fun batchSize(): Int =
    batchSize
  
  // Called from suspend poll
  @Synchronized
  fun addUncommitted(records: ConsumerRecords<*, *>) {
    records.partitions().forEach { tp: TopicPartition ->
      val offsets = uncommitted.computeIfAbsent(tp) { _: TopicPartition -> LinkedList() }
      records.records(tp).forEach { record ->
        if (!offsets.contains(record.offset())) {
          offsets.add(record.offset())
        }
      }
    }
  }
  
  // Called from ConsumerRebalanceListener, thus from our single threaded kafka dispatcher
  @Synchronized
  fun onPartitionsRevoked(revoked: Collection<TopicPartition>) {
    revoked.forEach(Consumer { part: TopicPartition ->
      uncommitted.remove(part)
      deferred.remove(part)
    })
  }
  
  // Called from suspend poll, and getAndClearOffsets
  @Synchronized
  fun deferredCount(): Int {
    var count = 0
    for (offsets in deferred.values) {
      count += offsets.size
    }
    return count
  }
  
  // Called from commit, which is only called from suspend poll/close code (running on single threaded kafka dispatcher)
  // **or** ConsumerRebalanceListener, thus from our single threaded kafka dispatcher
  @Synchronized
  fun getAndClearOffsets(): CommitArgs {
    val offsetMap: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
    if (outOfOrderCommits) {
      deferred.forEach { (tp: TopicPartition, offsets: List<Long>) ->
        if (offsets.size > 0) {
          offsets.sort()
          val uncomittedThisPart: MutableList<Long> = uncommitted[tp]!!
          var lastThisPart: Long = -1
          while (offsets.size > 0 && offsets[0] == uncomittedThisPart[0]) {
            lastThisPart = offsets[0]
            offsets.removeAt(0)
            uncomittedThisPart.removeAt(0)
          }
          if (lastThisPart >= 0) {
            offsetMap[tp] = OffsetAndMetadata(lastThisPart + 1)
          }
        }
      }
      batchSize = deferredCount()
    } else {
      latestOffsets.putAll(consumedOffsets)
      val iterator: MutableIterator<Map.Entry<TopicPartition, Long>> = consumedOffsets.entries.iterator()
      while (iterator.hasNext()) {
        val (key, value) = iterator.next()
        offsetMap[key] = OffsetAndMetadata(value + 1)
        iterator.remove()
      }
      batchSize = 0
    }
    val currentCallbackEmitters: MutableList<Continuation<Unit>>?
    if (continuations.isNotEmpty()) {
      currentCallbackEmitters = continuations
      continuations = ArrayList()
    } else currentCallbackEmitters = null
    return CommitArgs(offsetMap, currentCallbackEmitters)
  }
  
  // Called from commitFailure, could be `suspend` if `commit` becomes `suspend`.
  @Synchronized
  fun restoreOffsets(commitArgs: CommitArgs, restoreCallbackEmitters: Boolean) {
    // Restore offsets that haven't been updated.
    if (outOfOrderCommits) {
      commitArgs.offsets.forEach { (tp: TopicPartition, offset: OffsetAndMetadata) ->
        deferred[tp]!!
          .add(0, offset.offset() - 1)
        uncommitted[tp]!!.add(0, offset.offset() - 1)
      }
    } else {
      commitArgs.offsets.forEach { (topicPart, value) ->
        val offset = value.offset()
        val latestOffset = latestOffsets[topicPart]
        if (latestOffset == null || latestOffset <= offset - 1) consumedOffsets.putIfAbsent(topicPart, offset - 1)
      }
    }
    
    // If suspend failed after maxAttempts or due to fatal error, Continuation emitters are not restored.
    // A new suspend fun will result in a new Continuation
    // If suspend status is not being updated because commits are attempted again by KafkaReceiver,
    // restore the emitters for the next attempt.
    if (restoreCallbackEmitters && commitArgs.continuations != null) {
      continuations = commitArgs.continuations
    }
  }
  
  class CommitArgs internal constructor(
    val offsets: Map<TopicPartition, OffsetAndMetadata>,
    val continuations: MutableList<Continuation<Unit>>?,
  )
}
