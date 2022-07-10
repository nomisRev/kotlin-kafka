package io.github.nomisRev.kafka.reactor.internals

import io.github.nomisRev.kafka.ConsumerSettings
import io.github.nomisRev.kafka.custom.log
import io.github.nomisRev.kafka.kafkaConsumer
import io.github.nomisRev.kafka.reactor.ReceiverOffset
import io.github.nomisRev.kafka.reactor.ReceiverRecord
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.onClosed
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onCompletion
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
import java.time.Duration
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
        val loop = PollLoop(
          topicNames,
          settings,
          consumer,
          scope
        )
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
  private val committableBatch: CommittableBatch = CommittableBatch(),
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
  
  private val loop = EventLoop(
    ackMode,
    consumer,
    pollTimeout,
    maxDeferredCommits,
    isRetriableException,
    maxCommitAttempts,
    commitRetryInterval,
    scope,
    committableBatch,
    isActive,
    awaitingTransaction
  )
  private val commitJob = if (!commitInterval.isZero) {
    when (ackMode) {
      //AUTO_ACK,
      AckMode.MANUAL_ACK -> scope.launch(Dispatchers.Default) {
        while (true) {
          currentCoroutineContext().ensureActive()
          delay(commitInterval.toKotlinDuration())
          loop.commitEvent.scheduleIfRequired()
        }
      }
      
      else -> Job()
    }
  } else Job()
  
  fun receive(): Flow<ConsumerRecords<K, V>> = channelFlow {
    loop.init(this)
    loop.subscriber(topicNames)
    loop.poll()
    awaitClose()
  }.onCompletion { stop() }
  
  private suspend fun stop(): Unit {
    if (!isActive.compareAndSet(true, false)) Unit
    commitJob.cancel() // TODO test cancelAndJoin
    consumer.wakeup()
    loop.close(closeTimeout)
  }
  
  internal fun toCommittableOffset(record: ConsumerRecord<K, V>): CommittableOffset<K, V> =
    CommittableOffset(
      TopicPartition(record.topic(), record.partition()),
      record.offset(),
      loop.commitEvent,
      commitBatchSize
    )
}

internal class CommittableOffset<K, V>(
  private val topicPartition: TopicPartition,
  private val commitOffset: Long,
  private val commitEvent: EventLoop<K, V>.CommitEvent,
  private val commitBatchSize: Int,
) : ReceiverOffset {
  private val acknowledged = AtomicBoolean(false)
  
  override suspend fun commit(): Unit =
    if (maybeUpdateOffset() > 0) scheduleCommit() else Unit
  
  override fun acknowledge() {
    val uncommittedCount = maybeUpdateOffset().toLong()
    if (commitBatchSize > 0 && uncommittedCount >= commitBatchSize) commitEvent.scheduleIfRequired()
  }
  
  private fun maybeUpdateOffset(): Int =
    if (acknowledged.compareAndSet(false, true)) commitEvent.commitBatch.updateOffset(topicPartition, commitOffset)
    else commitEvent.commitBatch.batchSize()
  
  private suspend fun scheduleCommit(): Unit =
    suspendCoroutine { cont ->
      commitEvent.commitBatch.addCallbackEmitter(cont)
      commitEvent.scheduleIfRequired()
    }
  
  override fun topicPartition(): TopicPartition = topicPartition
  override fun offset(): Long = commitOffset
  override fun toString(): String = "$topicPartition@$commitOffset"
}

internal class EventLoop<K, V>(
  val ackMode: AckMode,
  val consumer: KafkaConsumer<K, V>,
  val pollTimeout: Duration,
  val maxDeferredCommits: Long,
  val isRetriableException: (Throwable) -> Boolean,
  val maxCommitAttempts: Int,
  val commitRetryInterval: Duration,
  val scope: CoroutineScope,
  val commitBatch: CommittableBatch,
  val isActive: AtomicBoolean,
  val awaitingTransaction: AtomicBoolean,
) {
  private val requesting = AtomicBoolean(true)
  private val pausedByUs = AtomicBoolean(false)
  
  val commitEvent = CommitEvent()
  lateinit var channel: ProducerScope<ConsumerRecords<K, V>>
  
  fun init(channel: ProducerScope<ConsumerRecords<K, V>>) {
    this@EventLoop.channel = channel
  }
  
  private fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
    log.debug("onPartitionsRevoked $partitions")
    if (!partitions.isEmpty()) {
      // It is safe to use the consumer here since we are in a poll(), so we can use safely use UNDISPATCHED.
      // if (ackMode != AckMode.ATMOST_ONCE){
      scope.launch(start = CoroutineStart.UNDISPATCHED) {
        commitEvent.runIfRequired(this, true)
      }
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
                val positions = partitions.map { part: TopicPartition? ->
                  "$part pos: ${consumer.position(part, Duration.ofSeconds(5))}"
                }
                log.trace(
                  "positions: $positions, committed: ${
                    consumer.committed(
                      HashSet(partitions), Duration.ofSeconds(5)
                    )
                  }"
                )
              } catch (ex: Exception) {
                log.error("Failed to get positions or committed", ex)
              }
            }
          }
        }
        
        override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
          log.debug("onPartitionsRevoked $partitions")
          commitEvent.commitBatch.partitionsRevoked(partitions)
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
    if (pausedNow && requesting.get() && !commitEvent.retrying.get()) {
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
  fun poll(): Job? =
    if (!scheduled.getAndSet(true)) scope.launch {
      try {
        scheduled.set(false)
        if (isActive.get()) {
          // Ensure that commits are not queued behind polls since number of poll events is chosen by reactor.
          commitEvent.runIfRequired(scope, false)
          
          val pauseForDeferred = (maxDeferredCommits > 0 && commitBatch.deferredCount() >= maxDeferredCommits)
          val shouldPoll: Boolean = if (pauseForDeferred || commitEvent.retrying.get()) false else requesting.get()
          
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
              commitEvent.retrying.get() -> log.debug("Paused - commits are retrying")
              else -> log.debug("Paused - back pressure")
            }
          }
          
          val records: ConsumerRecords<K, V> = try {
            consumer.poll(pollTimeout)
          } catch (e: WakeupException) {
            log.debug("Consumer woken")
            ConsumerRecords.empty()
          }
          if (isActive.get()) poll()
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
  
  inner class CommitEvent : suspend CoroutineScope.() -> Unit {
    val commitBatch: CommittableBatch = CommittableBatch()
    private val isPending = AtomicBoolean()
    private val inProgress = AtomicInteger()
    private val consecutiveCommitFailures = AtomicInteger()
    val retrying = AtomicBoolean()
    
    // We ignore this CoroutineScope since we want to schedule all task directly on the single thread parent scope
    override suspend fun invoke(ignored: CoroutineScope) {
      if (!isPending.compareAndSet(true, false)) return
      val commitArgs: CommittableBatch.CommitArgs = commitBatch.getAndClearOffsets()
      try {
        if (commitArgs.offsets.isEmpty()) handleSuccess(commitArgs, commitArgs.offsets)
        else {
          when (ackMode) {
            AckMode.MANUAL_ACK -> {
              inProgress.incrementAndGet()
              try {
                if (log.isDebugEnabled()) log.debug("Async committing: ${commitArgs.offsets}")
                consumer.commitAsync(commitArgs.offsets) { offsets, exception ->
                  inProgress.decrementAndGet()
                  if (exception == null) handleSuccess(commitArgs, offsets)
                  else handleFailure(commitArgs, exception)
                }
              } catch (e: Throwable) {
                inProgress.decrementAndGet()
                throw e
              }
              poll()
            }
          }
        }
      } catch (e: Exception) {
        log.error("Unexpected exception", e)
        handleFailure(commitArgs, e)
      }
    }
    
    suspend fun runIfRequired(scope: CoroutineScope, force: Boolean) {
      if (force) isPending.set(true)
      if (!retrying.get() && isPending.get()) invoke(scope)
    }
    
    private fun handleSuccess(
      commitArgs: CommittableBatch.CommitArgs?,
      offsets: Map<TopicPartition, OffsetAndMetadata>,
    ) {
      if (offsets.isNotEmpty()) consecutiveCommitFailures.set(0)
      pollTaskAfterRetry()
      if (commitArgs?.continuations != null) {
        commitArgs.continuations.forEach { cont ->
          cont.resume(Unit)
        }
      }
    }
    
    private fun handleFailure(
      commitArgs: CommittableBatch.CommitArgs,
      exception: Exception,
    ) {
      log.warn("Commit failed", exception)
      if (!isRetriableException(exception) && consecutiveCommitFailures.incrementAndGet() < maxCommitAttempts) {
        log.debug("Cannot retry")
        pollTaskAfterRetry()
        val callbackEmitters: List<Continuation<Unit>>? = commitArgs.continuations
        if (!callbackEmitters.isNullOrEmpty()) {
          isPending.set(false)
          commitBatch.restoreOffsets(commitArgs, false)
          callbackEmitters.forEach { cont ->
            cont.resumeWithException(exception)
          }
        } else {
          channel.close(exception)
        }
      } else {
        commitBatch.restoreOffsets(commitArgs, true)
        log.warn("Commit failed with exception $exception, retries remaining ${(maxCommitAttempts - consecutiveCommitFailures.get())}")
        isPending.set(true)
        retrying.set(true)
        poll()
        scope.launch {
          delay(commitRetryInterval.toKotlinDuration())
          invoke(this)
        }
      }
    }
    
    private fun pollTaskAfterRetry() {
      // if (log.isTraceEnabled()) log.trace("after retry ${retrying.get()}")
      if (retrying.getAndSet(false)) poll()
    }
    
    fun scheduleIfRequired(): Job? =
      if (isActive.get() && !retrying.get() && isPending.compareAndSet(false, true)) scope.launch { invoke(this) }
      else null
    
    fun waitFor(endTimeMillis: Long) {
      while (inProgress.get() > 0 && endTimeMillis - System.currentTimeMillis() > 0) {
        consumer.poll(Duration.ofMillis(1))
      }
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
        commitEvent.runIfRequired(this, forceCommit)
        commitEvent.waitFor(closeEndTimeMillis)
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