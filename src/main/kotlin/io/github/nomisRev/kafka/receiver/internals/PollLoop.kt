package io.github.nomisRev.kafka.receiver.internals

import io.github.nomisRev.kafka.receiver.Offset
import io.github.nomisRev.kafka.receiver.ReceiverRecord
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.github.nomisRev.kafka.receiver.internals.AckMode.EXACTLY_ONCE
import io.github.nomisRev.kafka.receiver.size
import java.time.Duration as JavaDuration
import java.time.Duration.ofSeconds
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.time.toJavaDuration
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
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.time.Duration

// TODO: Testing facility
private val DEBUG: Boolean = true
fun checkConsumerThread(msg: String): Unit =
  if (DEBUG) require(
    Thread.currentThread().name.startsWith("kotlin-kafka-")
  ) { "$msg => should run on kotlin-kafka thread, but found ${Thread.currentThread().name}" }
  else Unit

internal class PollLoop<K, V>(
  // TODO also allow for Pattern, and assign
  private val topicNames: Collection<String>,
  private val settings: ReceiverSettings<K, V>,
  private val consumer: Consumer<K, V>,
  private val scope: CoroutineScope,
  awaitingTransaction: AtomicBoolean = AtomicBoolean(false),
  private val isActive: AtomicBoolean = AtomicBoolean(true),
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
      settings.commitStrategy,
      reachedMaxCommitBatchSize,
      loop::scheduleCommitIfRequired
    )
  }

  fun receive(): Flow<ConsumerRecords<K, V>> {
    return loop.channel.consumeAsFlow()
      .onStart {
        println("############# onStart I AM ON: ${Thread.currentThread().name}")
        if (topicNames.isNotEmpty()) loop.subscribe(topicNames)
        loop.schedulePoll()
        commitManagerJob.start()
      }.onCompletion {
        println("############# onCompletion I AM ON: ${Thread.currentThread().name}.")
        stop()
      }
  }

  private suspend fun stop() = withContext(scope.coroutineContext) {
    isActive.set(false)
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
      settings.commitStrategy.size(),
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
    if (maybeUpdateOffset() > 0) suspendCoroutine { cont ->
      loop.commitBatch.addContinuation(cont)
      loop.scheduleCommitIfRequired()
    } else Unit

  override suspend fun acknowledge() {
    val uncommittedCount = maybeUpdateOffset().toLong()
    if (commitBatchSize in 1..uncommittedCount) {
      reachedMaxCommitBatchSize.send(Unit)
    }
  }

  private suspend fun maybeUpdateOffset(): Int =
    if (acknowledged.compareAndSet(false, true)) loop.commitBatch.updateOffset(topicPartition, offset)
    else loop.commitBatch.batchSize()

  override fun toString(): String = "$topicPartition@$offset"
}

private val logger: Logger =
  LoggerFactory.getLogger(EventLoop::class.java)

internal class EventLoop<K, V>(
  private val ackMode: AckMode,
  private val settings: ReceiverSettings<K, V>,
  private val consumer: Consumer<K, V>,
  private val isRetriableException: (Throwable) -> Boolean,
  private val scope: CoroutineScope,
  private val isActive: AtomicBoolean,
  private val awaitingTransaction: AtomicBoolean,
  private val atmostOnceOffsets: AtmostOnceOffsets,
) {
  /** Atomic state to check if we're poll'ing, or back-pressuring */
  private val isPolling = AtomicBoolean(true)

  /** Atomic state to tracks if we've paused, or not. */
  private val pausedByUs = AtomicBoolean(false)

  /** Channel to which we send records */
  val channel: Channel<ConsumerRecords<K, V>> = Channel()

  /** Cached pollTimeout, converted from [kotlin.time.Duration] to [java.time.Duration] */
  private val pollTimeout = settings.pollTimeout.toJavaDuration()

  /**
   * Subscribes to the given [topicNames],
   * and in case of failure we rethrow it through the [Channel] by closing with the caught exception.
   */
  suspend fun subscribe(topicNames: Collection<String>): Unit =
    withContext(scope.coroutineContext) {
      try {
        consumer.subscribe(topicNames, RebalanceListener())
      } catch (e: Throwable) {
        logger.error("Subscribing to $topicNames failed", e)
        val closed = channel.close(e)
        if (!closed) throw IllegalStateException("Failed to close EventLoop.channel")
          .apply { addSuppressed(e) }
      }
    }

  //<editor-fold desc="Polling">

  /**
   * Checks if we need to pause,
   * If it was already paused, then we check if we need to wake up the consumer.
   * We wake up the consumer, if we're actively polling records and we're currently not retrying sending commits.
   */
  @ConsumerThread
  private suspend fun pauseAndWakeupIfNeeded(): Boolean {
    checkConsumerThread("pauseAndWakeupIfNeeded")
    val pausedNow = !pausedByUs.getAndSet(true)
    val shouldWakeUpConsumer = pausedNow && isPolling.get() && !isRetryingCommit.get()
    logger.debug("checkAndSetPausedByUs: already paused {}, shouldWakeUpConsumer {}", pausedNow, shouldWakeUpConsumer)
    if (shouldWakeUpConsumer) {
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
          val shouldPoll: Boolean = if (pauseForDeferred || isRetryingCommit.get()) false else isPolling.get()

          if (shouldPoll) {
            if (!awaitingTransaction.get()) {
              if (pausedByUs.getAndSet(false)) {
                val toResume: MutableSet<TopicPartition> = HashSet(consumer.assignment())
                toResume.removeAll(pausedByUser)
                pausedByUser.clear()
                consumer.resume(toResume)
                logger.debug("Resumed partitions: {}", toResume)
              }
            } else {
              if (pauseAndWakeupIfNeeded()) {
                pausedByUser.addAll(consumer.paused())
                consumer.pause(consumer.assignment())
                logger.debug("Paused - awaiting transaction")
              }
            }
          } else if (pauseAndWakeupIfNeeded()) {
            pausedByUser.addAll(consumer.paused())
            consumer.pause(consumer.assignment())
            when {
              pauseForDeferred -> logger.debug("Paused - too many deferred commits")
              isRetryingCommit.get() -> logger.debug("Paused - commits are retrying")
              else -> logger.debug("Paused - back pressure")
            }
          }

          val records: ConsumerRecords<K, V> = try {
            println("############# consumer.poll I AM ON: ${Thread.currentThread().name}")
            consumer.poll(pollTimeout)
          } catch (e: WakeupException) {
            logger.debug("Consumer woken")
            ConsumerRecords.empty()
          }
          if (isActive.get()) schedulePoll()
          if (!records.isEmpty) {
            if (settings.maxDeferredCommits > 0) {
              commitBatch.addUncommitted(records)
            }
            logger.debug("Emitting ${records.count()} records")
            channel.trySend(records)
              .onClosed { error -> logger.error("Channel closed when trying to send records.", error) }
              .onFailure { error ->
                if (error != null) {
                  logger.error("Channel send failed when trying to send records.", error)
                  channel.close(error)
                } else logger.debug("Back-pressuring kafka consumer. Might pause KafkaConsumer on next tick.")

                isPolling.set(false)
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
                  isPolling.set(true)
                }
              }
          }
        }
      } catch (e: Exception) {
        logger.error("Unexpected exception", e)
        channel.close(e)
      }
    } else null

  /**
   * Callbacks are invoked on user thread
   */
  @ConsumerThread
  inner class RebalanceListener : ConsumerRebalanceListener {
    @ConsumerThread
    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
      checkConsumerThread("RebalanceListener.onPartitionsAssigned")
      logger.debug("onPartitionsAssigned {}", partitions)
      var repausedAll = false
      if (partitions.isNotEmpty() && pausedByUs.get()) {
        logger.debug("Rebalance during back pressure, re-pausing new assignments")
        consumer.pause(partitions)
        repausedAll = true
      }
      if (pausedByUser.isNotEmpty()) {
        val toRepause = buildList {
          //It is necessary to re-pause any user-paused partitions that are re-assigned after the rebalance.
          //Also remove any revoked partitions that the user paused from the userPaused collection.
          pausedByUser.forEach { tp ->
            if (partitions.contains(tp)) add(tp)
            else pausedByUser.remove(tp)
          }
        }
        if (!repausedAll && toRepause.isNotEmpty()) {
          consumer.pause(toRepause)
        }
      }
      // TODO Setup user listeners
      // for (onAssign in receiverOptions.assignListeners()) {
      //   onAssign.accept(toSeekable(partitions))
      // }
      traceCommitted(partitions)
    }

    @ConsumerThread
    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
      checkConsumerThread("RebalanceListener.onPartitionsRevoked")
      logger.debug("onPartitionsRevoked {}", partitions)
      revokePartitions(partitions)
      commitBatch.onPartitionsRevoked(partitions)
    }
  }

  @ConsumerThread
  private fun revokePartitions(partitions: Collection<TopicPartition>) {
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
  //</editor-fold>

  //<editor-fold desc="Comitting">
  val commitBatch: CommittableBatch = CommittableBatch()
  private val isPending = AtomicBoolean()
  private val commitsInProgress = AtomicInteger()
  private val consecutiveCommitFailures = AtomicInteger()
  private val isRetryingCommit = AtomicBoolean()

  /**
   * If we were retrying, schedule a poll and set isRetryingCommit to false
   * If we weren't retrying, do nothing.
   */
  private fun schedulePollAfterRetrying() {
    if (isRetryingCommit.getAndSet(false)) schedulePoll()
  }

  @ConsumerThread
  private fun runCommitIfRequired(force: Boolean) {
    if (force) isPending.set(true)
    if (!isRetryingCommit.get() && isPending.get()) commit()
  }

  fun scheduleCommitIfRequired(): Job? =
    if (isActive.get() && !isRetryingCommit.get() && isPending.compareAndSet(false, true)) scope.launch { commit() }
    else null


  // TODO  We should reset delay of commitJob
  @ConsumerThread
  private fun commit() {
    checkConsumerThread("commit")
    if (!isPending.compareAndSet(true, false)) return
    val commitArgs: CommittableBatch.CommitArgs = commitBatch.getAndClearOffsets()
    try {
      if (commitArgs.offsets.isEmpty()) commitSuccess(commitArgs, commitArgs.offsets)
      else {
        when (ackMode) {
          AckMode.MANUAL_ACK, AckMode.AUTO_ACK -> {
            commitsInProgress.incrementAndGet()
            try {
              logger.debug("Async committing: {}", commitArgs.offsets)
              consumer.commitAsync(commitArgs.offsets) { offsets, exception ->
                commitsInProgress.decrementAndGet()
                if (exception == null) commitSuccess(commitArgs, offsets)
                else commitFailure(commitArgs, exception)
              }
            } catch (e: Throwable) {
              commitsInProgress.decrementAndGet()
              // Rethrow, and invoke commitFailure at bottom of function
              throw e
            }
            schedulePoll()
          }

          AckMode.ATMOST_ONCE -> {
            logger.debug("Sync committing: {}", commitArgs.offsets)
            consumer.commitSync(commitArgs.offsets)
            commitSuccess(commitArgs, commitArgs.offsets)
            atmostOnceOffsets.onCommit(commitArgs.offsets)
          }
          /** [AckMode.EXACTLY_ONCE] offsets are committed by a producer. */
          EXACTLY_ONCE -> Unit
        }
      }
    } catch (e: Exception) {
      logger.error("Unexpected exception", e)
      commitFailure(commitArgs, e)
    }
  }

  /**
   * Commit was successfully:
   *   - Set commitFailures to 0
   *   - Schedule poll if we previously were retrying to commit
   *   - Complete all the [Offset.commit] continuations
   */
  @ConsumerThread
  private fun commitSuccess(
    commitArgs: CommittableBatch.CommitArgs?,
    offsets: Map<TopicPartition, OffsetAndMetadata>
  ) {
    checkConsumerThread("commitSuccess")
    if (offsets.isNotEmpty()) consecutiveCommitFailures.set(0)
    schedulePollAfterRetrying()
    commitArgs?.continuations?.forEach { cont ->
      cont.resume(Unit)
    }
  }

  @ConsumerThread
  private fun commitFailure(commitArgs: CommittableBatch.CommitArgs, exception: Exception) {
    checkConsumerThread("commitFailure")
    logger.warn("Commit failed", exception)
    if (!isRetriableException(exception) && consecutiveCommitFailures.incrementAndGet() < settings.maxCommitAttempts) {
      logger.debug("Commit failed with exception $exception, zero retries remaining")
      schedulePollAfterRetrying()
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
      isRetryingCommit.set(true)
      schedulePoll()
      scope.launch {
        delay(settings.commitRetryInterval)
        commit()
      }
    }
  }

  /**
   * Close the PollLoop,
   * and commit all outstanding [Offset.acknowledge] and [Offset.commit] before closing.
   *
   * It will make `3` attempts to commit the offsets,
   */
  suspend fun close(timeout: Duration): Unit = withContext(scope.coroutineContext) {
    checkConsumerThread("close")
//     val manualAssignment: Collection<TopicPartition> = receiverOptions.assignment()
//     if (!manualAssignment.isEmpty()) revokePartitions(manualAssignment)

    commitOnClose(
      closeEndTime = System.currentTimeMillis() + timeout.inWholeMilliseconds,
      maxAttempts = 3
    )
  }

  /**
   * We recurse here in case the consumer has had a recent wakeup call (from user code)
   * which will cause a poll() (in waitFor) to be interrupted while we're
   * possibly waiting for async commit results.
   */
  @ConsumerThread
  private suspend fun commitOnClose(closeEndTime: Long, maxAttempts: Int) {
    try {
      val forceCommit = when (ackMode) {
        AckMode.ATMOST_ONCE -> atmostOnceOffsets.undoCommitAhead(commitBatch)
        else -> true
      }
      //
      /**
       * [AckMode.EXACTLY_ONCE] offsets are committed by a producer, consumer may be closed immediately.
       * For all other [AckMode] we need to commit all [ReceiverRecord] [Offset.acknowledge].
       * Since we want to perform this in an optimal way
       */
      if (ackMode != EXACTLY_ONCE) {
        runCommitIfRequired(forceCommit)
        /**
         * The SDK doesn't send commit requests, unless we also poll.
         * So poll for 1 millis, until no more commitsInProgress.
         */
        while (commitsInProgress.get() > 0 && closeEndTime - System.currentTimeMillis() > 0) {
          consumer.poll(JavaDuration.ofMillis(1))
        }
      }
      val timeoutRemaining = closeEndTime - System.currentTimeMillis()
      consumer.close(JavaDuration.ofMillis(timeoutRemaining.coerceAtLeast(0)))
    } catch (e: WakeupException) {
      if (maxAttempts == 0) throw e
      else commitOnClose(closeEndTime, maxAttempts - 1)
    }
  }
  //</editor-fold>

  //<editor-fold desc="Logging">
  /** Trace the position, and last committed offsets for the given partitions */
  @ConsumerThread
  private fun traceCommitted(partitions: Collection<TopicPartition>) {
    if (logger.isTraceEnabled) {
      try {
        val positions = partitions.map { part: TopicPartition ->
          "$part position: ${consumer.position(part, ofSeconds(5))}"
        }

        val committed =
          consumer.committed(partitions.toSet(), ofSeconds(5))

        logger.trace("positions: $positions, committed: $committed")
      } catch (ex: Exception) {
        logger.error("Failed to get positions or committed", ex)
      }
    }
  }
  //</editor-fold>
}

/**
 * Documentation marker that functions that are only being called from the KafkaConsumer thread
 */
private annotation class ConsumerThread
