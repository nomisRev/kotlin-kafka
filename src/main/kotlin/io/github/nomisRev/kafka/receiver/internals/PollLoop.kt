package io.github.nomisRev.kafka.receiver.internals

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.Offset
import io.github.nomisRev.kafka.receiver.RebalanceStrategy
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.github.nomisRev.kafka.receiver.size
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.onClosed
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Duration.ofSeconds
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

internal class PollLoop<K, V>(
  // TODO also allow for Pattern, and assign
  private val topicNames: Collection<String>,
  private val settings: ReceiverSettings<K, V>,
  private val consumer: KafkaConsumer<K, V>,
  scope: CoroutineScope,
  awaitingTransaction: AtomicBoolean = AtomicBoolean(false),
  private val isActive: AtomicBoolean = AtomicBoolean(true),
  private val commitStrategy: CommitStrategy = CommitStrategy.BySizeOrTime(5, 5.seconds),
  private val ackMode: AckMode = AckMode.MANUAL_ACK,
  isRetriableException: (Throwable) -> Boolean = { e -> e is RetriableCommitFailedException },
) {
  private val reachedMaxCommitBatchSize = Channel<Unit>(Channel.RENDEZVOUS)
  private val atMostOnceOffset: AtmostOnceOffsets = AtmostOnceOffsets()
  private val loop = EventLoop(
    ackMode,
    settings,
    consumer,
    isRetriableException,
    scope,
    isActive,
    awaitingTransaction,
    atMostOnceOffset
  )
  
  /*
   * Takes care of scheduling our commits to Kafka.
   * It will schedule a commit after `reachedMaxCommitBatchSize` channel signals it has reach the batch size,
   * or when it times out after the given `commitInterval`.
   * This way it optimises sending commits to Kafka in an optimised way.
   * Either every `Duration` or `x` elements, whichever comes first.
   */
  private val commitManagerJob = scope.launch(
    start = CoroutineStart.LAZY,
    context = Dispatchers.Default
  ) {
    offsetCommitWorker(
      ackMode,
      commitStrategy,
      reachedMaxCommitBatchSize,
      loop::scheduleCommitIfRequired
    )
  }
  
  fun receive(): Flow<ConsumerRecords<K, V>> =
    loop.channel.consumeAsFlow()
      .onStart {
        if (topicNames.isNotEmpty()) loop.subscriber(topicNames)
        loop.schedulePoll()
        commitManagerJob.start()
      }.onCompletion { stop() }
  
  private suspend fun stop() {
    if (!isActive.compareAndSet(true, false)) Unit
    reachedMaxCommitBatchSize.close()
    commitManagerJob.cancelAndJoin()
    consumer.wakeup()
    loop.close(settings.closeTimeout)
  }
  
  internal fun toCommittableOffset(record: ConsumerRecord<K, V>): CommittableOffset<K, V> =
    CommittableOffset(
      TopicPartition(record.topic(), record.partition()),
      record.offset(),
      loop,
      commitStrategy.size(),
      reachedMaxCommitBatchSize
    )
}

internal class CommittableOffset<K, V>(
  override val topicPartition: TopicPartition,
  override val offset: Long,
  private val loop: EventLoop<K, V>,
  private val commitBatchSize: Int,
  private val reachedMaxCommitBatchSize: Channel<Unit>,
) : Offset {
  private val acknowledged = AtomicBoolean(false)
  
  override suspend fun commit(): Unit =
    if (maybeUpdateOffset() > 0) scheduleCommit() else Unit
  
  override suspend fun acknowledge() {
    val uncommittedCount = maybeUpdateOffset().toLong()
    if (commitBatchSize in 1..uncommittedCount) {
      reachedMaxCommitBatchSize.send(Unit)
    }
  }
  
  private /*suspend*/ fun maybeUpdateOffset(): Int =
    if (acknowledged.compareAndSet(false, true)) loop.commitBatch.updateOffset(topicPartition, offset)
    else loop.commitBatch.batchSize()
  
  private suspend fun scheduleCommit(): Unit =
    suspendCoroutine { cont ->
      loop.commitBatch.addContinuation(cont)
      loop.scheduleCommitIfRequired()
    }
  
  override fun toString(): String = "$topicPartition@$offset"
}

private val logger: Logger =
  LoggerFactory.getLogger(EventLoop::class.java)

internal class EventLoop<K, V>(
  private val ackMode: AckMode,
  private val settings: ReceiverSettings<K, V>,
  private val consumer: KafkaConsumer<K, V>,
  private val isRetriableException: (Throwable) -> Boolean,
  private val scope: CoroutineScope,
  private val isActive: AtomicBoolean,
  private val awaitingTransaction: AtomicBoolean,
  private val atmostOnceOffsets: AtmostOnceOffsets,
) {
  private val requesting = AtomicBoolean(true)
  private val pausedByUs = AtomicBoolean(false)
  val channel: Channel<ConsumerRecords<K, V>> = Channel()
  private val pollTimeout = settings.pollTimeout.toJavaDuration()
  
  private fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
    if (!partitions.isEmpty()) {
      // It is safe to use the consumer here since we are in a poll()
      if (ackMode != AckMode.ATMOST_ONCE) {
        runCommitIfRequired(true)
      }
      // TODO Setup user listeners
      // for (onRevoke in receiverOptions.revokeListeners()) {
      //   onRevoke.accept(toSeekable(partitions))
      // }
    }
  }
  
  fun subscriber(topicNames: Collection<String>): Job = scope.launch {
    try {
      consumer.subscribe(topicNames, object : ConsumerRebalanceListener {
        override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
          logger.debug("onPartitionsAssigned $partitions")
          // onAssign methods may perform seek. It is safe to use the consumer here since we are in a poll()
          if (partitions.isNotEmpty()) {
            if (pausedByUs.get()) {
              logger.debug("Rebalance during back pressure, re-pausing new assignments")
              consumer.pause(partitions)
            }
            // TODO Setup user listeners
            // for (onAssign in receiverOptions.assignListeners()) {
            //   onAssign.accept(toSeekable(partitions))
            // }
            if (logger.isTraceEnabled) {
              try {
                val positions = partitions.map { part: TopicPartition ->
                  "$part pos: ${consumer.position(part, ofSeconds(5))}"
                }
                logger.trace(
                  "positions: $positions, committed: ${
                    consumer.committed(
                      partitions.toSet(),
                      ofSeconds(5)
                    )
                  }"
                )
              } catch (ex: Exception) {
                logger.error("Failed to get positions or committed", ex)
              }
            }
          }
        }
        
        override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
          logger.debug("onPartitionsRevoked $partitions")
          commitBatch.onPartitionsRevoked(partitions)
          this@EventLoop.onPartitionsRevoked(partitions)
        }
      })
    } catch (e: Throwable) {
      logger.error("Unexpected exception", e)
      channel.close(e)
    }
  }
  
  /**
   * Returns `true` if you need to _pause_ the consumer, otherwise the consumer was already paused.
   *
   * If the consumer meets the following conditions, we should wakeup the consumer:
   *  - paused
   *  - The downstream can continue processing, requesting.get() == true
   *  - and we're not in retrying commits
   */
  private fun checkAndSetPausedByUs(): Boolean {
    logger.debug("checkAndSetPausedByUs")
    val pausedNow = !pausedByUs.getAndSet(true)
    if (pausedNow && requesting.get() && !retrying.get()) {
      consumer.wakeup()
    }
    return pausedNow
  }
  
  /*
   * TODO this can probably be removed
   * Race condition where onRequest was called to increase requested but we
   * hadn't yet paused the consumer; wake immediately in this case.
   */
  private val scheduled = AtomicBoolean()
  private val pausedByUser: MutableSet<TopicPartition> = HashSet()
  fun schedulePoll(): Job? =
    if (!scheduled.getAndSet(true)) scope.launch {
      try {
        scheduled.set(false)
        if (isActive.get()) {
          // Ensure that commits are not queued behind polls since number of poll events is chosen by reactor.
          runCommitIfRequired(false)
          
          val pauseForDeferred =
            (settings.maxDeferredCommits > 0 && commitBatch.deferredCount() >= settings.maxDeferredCommits)
          
          val shouldPoll: Boolean =
            if (pauseForDeferred || retrying.get()) false
            else requesting.get()
          
          when {
            shouldPoll && !awaitingTransaction.get() && pausedByUs.getAndSet(false) -> {
              val toResume: MutableSet<TopicPartition> = HashSet(consumer.assignment())
              toResume.removeAll(pausedByUser)
              pausedByUser.clear()
              consumer.resume(toResume)
              logger.debug("Resumed")
            }
            
            shouldPoll && awaitingTransaction.get() && checkAndSetPausedByUs() -> {
              pausedByUser.addAll(consumer.paused())
              consumer.pause(consumer.assignment())
              logger.debug("Paused - awaiting transaction")
            }
  
            // !shouldPoll so we need to apply backpressure to Kafka according to strategy
            !shouldPoll && checkAndSetPausedByUs() ->
              when (val strategy = settings.rebalanceStrategy) {
                RebalanceStrategy.Backpressure -> {
                  pausedByUser.addAll(consumer.paused())
                  consumer.pause(consumer.assignment())
                  when {
                    pauseForDeferred -> logger.debug("Paused - too many deferred commits")
                    retrying.get() -> logger.debug("Paused - commits are retrying")
                    else -> logger.debug("Paused - back pressure")
                  }
                }
                
                is RebalanceStrategy.FailFast -> strategy.failFast(consumer.assignment())
              }
          }
          
          // Execute poll
          val records: ConsumerRecords<K, V> = try {
            consumer.poll(pollTimeout)
          } catch (e: WakeupException) {
            logger.debug("Consumer woken")
            ConsumerRecords.empty()
          }
          
          // Schedule a new poll task on the single threaded dispatcher
          if (isActive.get()) schedulePoll()
          
          // Send the records downstream, and check state of buffer. Signal back-pressure in `requesting`
          if (!records.isEmpty) {
            if (settings.maxDeferredCommits > 0) {
              commitBatch.addUncommitted(records)
            }
            logger.debug("Emitting ${records.count()} records")
            channel.trySend(records)
              .onClosed { logger.error("Channel closed when trying to send records.", it) }
              .onFailure { error ->
                if (error != null) {
                  logger.error("Channel send failed when trying to send records.", error)
                  channel.close(error)
                } else logger.debug("Back-pressuring kafka consumer. Might pause KafkaConsumer on next tick.")
                
                requesting.set(false)
                // TODO Can we rely on a dispatcher from above?
                //      This should not run on the kafka consumer thread
                scope.launch(Dispatchers.Default) {
                  /*
                   * Send the records down,
                   * when send returns we attempt to send and empty set of records down to test the backpressure.
                   * If our "backpressure test" returns we start requesting/polling again
                   */
                  channel.send(records)
                  channel.send(ConsumerRecords.empty())
                  if (pausedByUs.get()) {
                    consumer.wakeup()
                  }
                  requesting.set(true)
                }
              }
          }
        }
      } catch (e: Exception) {
        logger.error("Unexpected exception", e)
        channel.close(e)
      }
    } else null
  
  val commitBatch: CommittableBatch = CommittableBatch()
  private val isPending = AtomicBoolean()
  private val inProgress = AtomicInteger()
  private val consecutiveCommitFailures = AtomicInteger()
  private val retrying = AtomicBoolean()
  
  // TODO Should reset delay of commitJob
  private fun commit() {
    if (!isPending.compareAndSet(true, false)) return
    val commitArgs: CommittableBatch.CommitArgs = commitBatch.getAndClearOffsets()
    try {
      if (commitArgs.offsets.isEmpty()) commitSuccess(commitArgs, commitArgs.offsets)
      else {
        when (ackMode) {
          AckMode.MANUAL_ACK, AckMode.AUTO_ACK -> {
            inProgress.incrementAndGet()
            try {
              logger.debug("Async committing: ${commitArgs.offsets}")
              consumer.commitAsync(commitArgs.offsets) { offsets, exception ->
                inProgress.decrementAndGet()
                if (exception == null) commitSuccess(commitArgs, offsets)
                else commitFailure(commitArgs, exception)
              }
            } catch (e: Throwable) {
              inProgress.decrementAndGet()
              throw e
            }
            schedulePoll()
          }
          
          AckMode.ATMOST_ONCE -> {
            logger.debug("Sync committing: ${commitArgs.offsets}")
            consumer.commitSync(commitArgs.offsets)
            commitSuccess(commitArgs, commitArgs.offsets)
            atmostOnceOffsets.onCommit(commitArgs.offsets)
          }
          // Handled separately using transactional KafkaSender
          AckMode.EXACTLY_ONCE -> Unit
        }
      }
    } catch (e: Exception) {
      logger.error("Unexpected exception", e)
      commitFailure(commitArgs, e)
    }
  }
  
  private fun commitSuccess(commitArgs: CommittableBatch.CommitArgs?, offsets: Map<TopicPartition, OffsetAndMetadata>) {
    if (offsets.isNotEmpty()) {
      consecutiveCommitFailures.set(0)
    }
    pollTaskAfterRetry()
    commitArgs?.continuations?.forEach { cont ->
      cont.resume(Unit)
    }
  }
  
  private fun pollTaskAfterRetry(): Job? =
    if (retrying.getAndSet(false)) schedulePoll() else null
  
  private fun commitFailure(commitArgs: CommittableBatch.CommitArgs, exception: Exception) {
    logger.warn("Commit failed", exception)
    if (!isRetriableException(exception) && consecutiveCommitFailures.incrementAndGet() < settings.maxCommitAttempts) {
      logger.debug("Cannot retry")
      pollTaskAfterRetry()
      val callbackEmitters: List<Continuation<Unit>>? = commitArgs.continuations
      if (callbackEmitters.isNullOrEmpty()) channel.close(exception)
      else {
        isPending.set(false)
        commitBatch.restoreOffsets(commitArgs, false)
        callbackEmitters.forEach { cont ->
          cont.resumeWithException(exception)
        }
      }
    } else {
      commitBatch.restoreOffsets(commitArgs, true)
      logger.warn("Commit failed with exception $exception, retries remaining ${(settings.maxCommitAttempts - consecutiveCommitFailures.get())}")
      isPending.set(true)
      retrying.set(true)
      schedulePoll()
      scope.launch {
        delay(settings.commitRetryInterval)
        commit()
      }
    }
  }
  
  private fun runCommitIfRequired(force: Boolean) {
    if (force) isPending.set(true)
    if (!retrying.get() && isPending.get()) commit()
  }
  
  fun scheduleCommitIfRequired(): Job? =
    if (isActive.get() && !retrying.get() && isPending.compareAndSet(false, true)) scope.launch { commit() }
    else null
  
  // TODO investigate
  //  https://github.com/akka/alpakka-kafka/blob/aad6a1ccbd4f549b3053988c85cbbe9b11d51542/core/src/main/scala/akka/kafka/internal/KafkaConsumerActor.scala#L514
  private fun waitFor(endTimeMillis: Long) {
    while (inProgress.get() > 0 && endTimeMillis - System.currentTimeMillis() > 0) {
      consumer.poll(Duration.ofMillis(1))
    }
  }
  
  suspend fun close(timeout: kotlin.time.Duration): Unit = withContext(scope.coroutineContext) {
    val closeEndTimeMillis = System.currentTimeMillis() + timeout.inWholeMilliseconds
    // val manualAssignment: Collection<TopicPartition> = receiverOptions.assignment()
    // if (manualAssignment != null && !manualAssignment.isEmpty()) onPartitionsRevoked(manualAssignment)
    /*
     * We loop here in case the consumer has had a recent wakeup call (from user code)
     * which will cause a poll() (in waitFor) to be interrupted while we're
     * possibly waiting for async commit results.
     */
    val maxAttempts = 3
    for (i in 0 until maxAttempts) {
      try {
        val forceCommit = when (ackMode) {
          AckMode.ATMOST_ONCE -> atmostOnceOffsets.undoCommitAhead(commitBatch)
          else -> true
        }
        // For exactly-once, offsets are committed by a producer, consumer may be closed immediately
        if (ackMode != AckMode.EXACTLY_ONCE) {
          runCommitIfRequired(forceCommit)
          waitFor(closeEndTimeMillis)
        }
        var timeoutMillis: Long = closeEndTimeMillis - System.currentTimeMillis()
        if (timeoutMillis < 0) timeoutMillis = 0
        consumer.close(Duration.ofMillis(timeoutMillis))
        break
      } catch (e: WakeupException) {
        if (i == maxAttempts - 1) throw e
      }
    }
  }
}