package io.github.nomisRev.kafka.consumer.internals

import io.github.nomisRev.kafka.ConsumerSettings
import io.github.nomisRev.kafka.kafkaConsumer
import io.github.nomisRev.kafka.consumer.Offset
import io.github.nomisRev.kafka.consumer.ReceiverRecord
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.onClosed
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.whileSelect
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import java.time.Duration
import java.time.Duration.ofSeconds
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.time.toKotlinDuration

public object KConsumer {
  
  @FlowPreview
  public fun <K, V> subscribe(
    settings: ConsumerSettings<K, V>,
    topicNames: Collection<String>,
  ): Flow<ReceiverRecord<K, V>> =
    kafkaScheduler(settings.groupId).flatMapConcat { (scope, dispatcher) ->
      kafkaConsumer(settings).flatMapConcat { consumer ->
        val loop = PollLoop(topicNames, settings, consumer, scope)
        loop.receive().flatMapConcat { records ->
          records.map { record ->
            ReceiverRecord(record, loop.toCommittableOffset(record))
          }.asFlow()
        }.flowOn(dispatcher)
      }
    }
}

internal class PollLoop<K, V>(
  private val topicNames: Collection<String>,
  private val receiverOptions: ConsumerSettings<K, V>,
  private val consumer: KafkaConsumer<K, V>,
  private val scope: CoroutineScope,
  private val awaitingTransaction: AtomicBoolean = AtomicBoolean(false),
  private val isActive: AtomicBoolean = AtomicBoolean(true),
  private val commitBatchSize: Int = 5,
  private val ackMode: AckMode = AckMode.MANUAL_ACK,
  private val pollTimeout: Duration = Duration.ofMillis(100),
  private val maxDeferredCommits: Long = 0L,
  private val isRetriableException: (Throwable) -> Boolean = { e -> e is RetriableCommitFailedException },
  private val maxCommitAttempts: Int = 100,
  private val commitRetryInterval: Duration = Duration.ofMillis(500),
  private val commitInterval: Duration = Duration.ofMillis(5000),
  private val closeTimeout: Duration = Duration.ofMillis(Long.MAX_VALUE),
) {
  private val reachedMaxCommitBatchSize = Channel<Unit>(Channel.RENDEZVOUS)
  private val loop = EventLoop(
    ackMode,
    consumer,
    pollTimeout,
    maxDeferredCommits,
    isRetriableException,
    maxCommitAttempts,
    commitRetryInterval,
    scope,
    isActive,
    awaitingTransaction
  )
  
  /*
   * Takes care of scheduling our commits to Kafka.
   * It will schedule a commit after `reachedMaxCommitBatchSize` channel signals it has reach the batch size,
   * or when it times out after the given `commitInterval`.
   * This way it optimises sending commits to Kafka in an optimised way.
   * Either every `Duration` or `x` elements, whichever comes first.
   */ // TODO have more strategies. Can still commit in `n` sized batches for `commitInterval.isZero`.
  private val commitManagerJob = if (!commitInterval.isZero) {
    when (ackMode) {
      //AUTO_ACK,
      AckMode.MANUAL_ACK -> scope.launch(start = CoroutineStart.LAZY, context = Dispatchers.Default) {
        whileSelect {
          reachedMaxCommitBatchSize.onReceiveCatching {
            loop.scheduleCommitIfRequired()
            !it.isClosed // Stop on close
          }
          
          onTimeout(commitInterval.toKotlinDuration()) {
            loop.scheduleCommitIfRequired()
            true
          }
        }
      }
      else -> Job()
    }
  } else Job()
  
  fun receive(): Flow<ConsumerRecords<K, V>> =
    loop.channel.consumeAsFlow()
      .onStart {
        loop.subscriber(topicNames)
        loop.schedulePoll()
        commitManagerJob.start()
      }.onCompletion { stop() }
  
  private suspend fun stop() {
    if (!isActive.compareAndSet(true, false)) Unit
    reachedMaxCommitBatchSize.close()
    commitManagerJob.cancel() // TODO test cancelAndJoin
    consumer.wakeup()
    loop.close(closeTimeout)
  }
  
  internal fun toCommittableOffset(record: ConsumerRecord<K, V>): CommittableOffset<K, V> =
    CommittableOffset(
      TopicPartition(record.topic(), record.partition()),
      record.offset(),
      loop,
      commitBatchSize,
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
    if (commitBatchSize > 0 && uncommittedCount >= commitBatchSize) {
      reachedMaxCommitBatchSize.send(Unit)
    }
  }
  
  private fun maybeUpdateOffset(): Int =
    if (acknowledged.compareAndSet(false, true)) loop.commitBatch.updateOffset(topicPartition, offset)
    else loop.commitBatch.batchSize()
  
  private suspend fun scheduleCommit(): Unit =
    suspendCoroutine { cont ->
      loop.commitBatch.addContinuation(cont)
      loop.scheduleCommitIfRequired()
    }
  
  override fun toString(): String = "$topicPartition@$offset"
}

internal class EventLoop<K, V>(
  private val ackMode: AckMode,
  private val consumer: KafkaConsumer<K, V>,
  private val pollTimeout: Duration,
  private val maxDeferredCommits: Long,
  private val isRetriableException: (Throwable) -> Boolean,
  private val maxCommitAttempts: Int,
  private val commitRetryInterval: Duration,
  private val scope: CoroutineScope,
  private val isActive: AtomicBoolean,
  private val awaitingTransaction: AtomicBoolean,
) {
  private val requesting = AtomicBoolean(true)
  private val pausedByUs = AtomicBoolean(false)
  val channel: Channel<ConsumerRecords<K, V>> = Channel()
  
  private fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
    log.debug("onPartitionsRevoked $partitions")
    if (!partitions.isEmpty()) {
      // It is safe to use the consumer here since we are in a poll()
      // if (ackMode != AckMode.ATMOST_ONCE){
      runCommitIfRequired(true)
      // }
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
          log.debug("onPartitionsAssigned $partitions")
          // onAssign methods may perform seek. It is safe to use the consumer here since we are in a poll()
          if (partitions.isNotEmpty()) {
            if (pausedByUs.get()) {
              log.debug("Rebalance during back pressure, re-pausing new assignments")
              consumer.pause(partitions)
            }
            // TODO Setup user listeners
            // for (onAssign in receiverOptions.assignListeners()) {
            //   onAssign.accept(toSeekable(partitions))
            // }
            if (log.isTraceEnabled()) {
              try {
                val positions = partitions.map { part: TopicPartition ->
                  "$part pos: ${consumer.position(part, ofSeconds(5))}"
                }
                log.trace("positions: $positions, committed: ${consumer.committed(partitions.toSet(), ofSeconds(5))}")
              } catch (ex: Exception) {
                log.error("Failed to get positions or committed", ex)
              }
            }
          }
        }
        
        override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
          log.debug("onPartitionsRevoked $partitions")
          commitBatch.partitionsRevoked(partitions)
          this@EventLoop.onPartitionsRevoked(partitions)
        }
      })
    } catch (e: Throwable) {
      log.error("Unexpected exception", e)
      channel.close(e)
    }
  }
  
  private fun checkAndSetPausedByUs(): Boolean {
    log.debug("checkAndSetPausedByUs")
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
          
          val pauseForDeferred = (maxDeferredCommits > 0 && commitBatch.deferredCount() >= maxDeferredCommits)
          val shouldPoll: Boolean = if (pauseForDeferred || retrying.get()) false else requesting.get()
          
          if (shouldPoll) {
            if (!awaitingTransaction.get()) {
              if (pausedByUs.getAndSet(false)) {
                val toResume: MutableSet<TopicPartition> = HashSet(consumer.assignment())
                toResume.removeAll(pausedByUser)
                pausedByUser.clear()
                consumer.resume(toResume)
                log.debug("Resumed")
              }
            } else {
              if (checkAndSetPausedByUs()) {
                pausedByUser.addAll(consumer.paused())
                consumer.pause(consumer.assignment())
                log.debug("Paused - awaiting transaction")
              }
            }
          } else if (checkAndSetPausedByUs()) {
            pausedByUser.addAll(consumer.paused())
            consumer.pause(consumer.assignment())
            when {
              pauseForDeferred -> log.debug("Paused - too many deferred commits")
              retrying.get() -> log.debug("Paused - commits are retrying")
              else -> log.debug("Paused - back pressure")
            }
          }
          
          val records: ConsumerRecords<K, V> = try {
            consumer.poll(pollTimeout)
          } catch (e: WakeupException) {
            log.debug("Consumer woken")
            ConsumerRecords.empty()
          }
          if (isActive.get()) schedulePoll()
          if (!records.isEmpty) {
            if (maxDeferredCommits > 0) {
              commitBatch.addUncommitted(records)
            }
            log.debug("Emitting ${records.count()} records")
            channel.trySend(records)
              .onClosed { log.error("Channel closed when trying to send records.", it) }
              .onFailure { error ->
                if (error != null) {
                  log.error("Channel send failed when trying to send records.", error)
                  channel.close(error)
                } else log.debug("Back-pressuring kafka consumer. Might pause KafkaConsumer on next tick.")
                
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
        log.error("Unexpected exception", e)
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
          AckMode.MANUAL_ACK -> {
            inProgress.incrementAndGet()
            try {
              if (log.isDebugEnabled()) log.debug("Async committing: ${commitArgs.offsets}")
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
        }
      }
    } catch (e: Exception) {
      log.error("Unexpected exception", e)
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
    log.warn("Commit failed", exception)
    if (!isRetriableException(exception) && consecutiveCommitFailures.incrementAndGet() < maxCommitAttempts) {
      log.debug("Cannot retry")
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
      log.warn("Commit failed with exception $exception, retries remaining ${(maxCommitAttempts - consecutiveCommitFailures.get())}")
      isPending.set(true)
      retrying.set(true)
      schedulePoll()
      scope.launch {
        delay(commitRetryInterval.toKotlinDuration())
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
  
  private fun waitFor(endTimeMillis: Long) {
    while (inProgress.get() > 0 && endTimeMillis - System.currentTimeMillis() > 0) {
      consumer.poll(Duration.ofMillis(1))
    }
  }
  
  suspend fun close(timeout: Duration): Unit = withContext(scope.coroutineContext) {
    val closeEndTimeMillis = System.currentTimeMillis() + timeout.toMillis()
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
        var forceCommit = true
        // if (ackMode == reactor.kafka.receiver.internals.AckMode.ATMOST_ONCE){
        //   forceCommit = atmostOnceOffsets.undoCommitAhead(commitEvent.commitBatch)
        // }
        // For exactly-once, offsets are committed by a producer, consumer may be closed immediately
        // if (ackMode != reactor.kafka.receiver.internals.AckMode.EXACTLY_ONCE) {
        runCommitIfRequired(forceCommit)
        waitFor(closeEndTimeMillis)
        // }
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