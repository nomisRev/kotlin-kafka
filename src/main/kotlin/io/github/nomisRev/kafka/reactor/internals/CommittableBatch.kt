package io.github.nomisRev.kafka.reactor.internals

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.LinkedList
import java.util.function.Consumer
import kotlin.coroutines.Continuation

// TODO Replaced @Synchronized with Mutex...
// Can we do something else? Perhaps keep track of this in an Atomic inside of the run loop?
// Replace atomic state inside of this class with AtomicRef instead of locking/synchronized
internal class CommittableBatch {
  val consumedOffsets: MutableMap<TopicPartition, Long> = HashMap()
  val uncommitted: MutableMap<TopicPartition, MutableList<Long>> = HashMap()
  val deferred: MutableMap<TopicPartition, MutableList<Long>> = HashMap()
  private val latestOffsets: MutableMap<TopicPartition, Long> = HashMap()
  
  var outOfOrderCommits = false
  private var batchSize = 0
  private var callbackEmitters: MutableList<Continuation<Unit>> = ArrayList()
  private val mutex: Mutex = Mutex()
  
  @Synchronized
  fun updateOffset(topicPartition: TopicPartition, offset: Long): Int {
    if (outOfOrderCommits) {
      val uncommittedThisTP: List<Long>? = uncommitted[topicPartition]
      if (uncommittedThisTP != null && uncommittedThisTP.contains(offset)) {
        val offsets = deferred.computeIfAbsent(
          topicPartition
        ) { tp: TopicPartition? -> LinkedList() }
        if (!offsets.contains(offset)) {
          offsets.add(offset)
          batchSize++
        }
      } else {
        // TODO logger info
        //log.debug("No uncomitted offset for $topicPartition@$offset, partition revoked?")
      }
    } else if (offset != consumedOffsets.put(topicPartition, offset)) {
      batchSize++
    }
    return batchSize
  }
  
  @Synchronized
  fun addCallbackEmitter(emitter: Continuation<Unit>) =
    callbackEmitters.add(emitter)
  
  suspend fun isEmpty(): Boolean = mutex.withLock {
    batchSize == 0
  }
  
  @Synchronized
  fun batchSize(): Int =
    batchSize
  
  suspend fun addUncommitted(records: ConsumerRecords<*, *>) = mutex.withLock {
    records.partitions().forEach { tp: TopicPartition ->
      val offsets = uncommitted.computeIfAbsent(tp) { _: TopicPartition -> LinkedList() }
      records.records(tp).forEach { record ->
        if (!offsets.contains(record.offset())) {
          offsets.add(record.offset())
        }
      }
    }
  }
  
  @Synchronized
  fun partitionsRevoked(revoked: Collection<TopicPartition>){
    revoked.forEach(Consumer { part: TopicPartition ->
      uncommitted.remove(part)
      deferred.remove(part)
    })
  }
  
  suspend fun deferredCount(): Int = mutex.withLock {
    var count = 0
    for (offsets in deferred.values) {
      count += offsets.size
    }
    count
  }
  
  suspend fun getAndClearOffsets(): CommitArgs = mutex.withLock {
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
    if (callbackEmitters.isNotEmpty()) {
      currentCallbackEmitters = callbackEmitters
      callbackEmitters = ArrayList()
    } else currentCallbackEmitters = null
    CommitArgs(offsetMap, currentCallbackEmitters)
  }
  
  
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
    // If Mono is being failed after maxAttempts or due to fatal error, callback emitters
    // are not restored. Mono#retry will generate new callback emitters. If Mono status
    // is not being updated because commits are attempted again by KafkaReceiver, restore
    // the emitters for the next attempt.
    if (restoreCallbackEmitters && commitArgs.continuations != null) callbackEmitters = commitArgs.continuations
  }
  
  // @Synchronized
  override fun toString(): String =
    consumedOffsets.toString()
  
  class CommitArgs internal constructor(
    val offsets: Map<TopicPartition, OffsetAndMetadata>,
    val continuations: MutableList<Continuation<Unit>>?,
  )
  
  companion object {
    // private val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(CommittableBatch::class.java)
  }
}
