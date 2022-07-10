package io.github.nomisRev.kafka.reactor.internals

import io.github.nomisRev.kafka.ConsumerSettings
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.invoke
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

/**
 * Since {@link org.apache.kafka.clients.consumer.Consumer} does not support multithreaded access,
 * this event loop serializes every action we perform on it.
 */
internal class ConsumerEventLoop<K, V>(
  val ackMode: AckMode,
  val atmostOnceOffsets: AtmostOnceOffsets,
  val consumerSettings: ConsumerSettings<K, V>,
  val eventDispatcher: ExecutorCoroutineDispatcher,
  val consumer: KafkaConsumer<K, V>,
  val isRetriableException: (Throwable) -> Boolean,
  val collector: Channel<ConsumerRecord<K, V>>,
  val awaitingTransaction: AtomicBoolean,
) : CoroutineScope by CoroutineScope(eventDispatcher) {
  val isActive = AtomicBoolean(true)
  val commitEvent: CommitEvent = CommitEvent()
  val pollEvent = PollEvent()
  
  suspend fun request() {
    // if (ConsumerEventLoop.log.isDebugEnabled()) {
    //   ConsumerEventLoop.log.debug("onRequest.toAdd $toAdd, paused ${pollEvent.isPaused()}")
    // }
    if (pollEvent.isPaused()) consumer.wakeup()
    pollEvent.schedule()
  }
  
  // TODO FIX
  private suspend fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
    // ConsumerEventLoop.log.debug("onPartitionsRevoked {}", partitions)
    if (partitions.isNotEmpty()) {
      // It is safe to use the consumer here since we are in a poll()
      /*if (ackMode != AckMode.ATMOST_ONCE)*/ commitEvent.runIfRequired(true)
      // for (onRevoke in consumerSettings.revokeListeners()) {
      //   onRevoke.accept(toSeekable(partitions))
      // }
    }
  }
  
  suspend fun stop(): Unit {
    try {
      // ConsumerEventLoop.log.debug("dispose $isActive")
      if (!isActive.compareAndSet(true, false)) return Unit
      // periodicCommitDisposable.cancelAndJoin()
      consumer.wakeup()
      eventDispatcher.invoke {
        close(1.seconds /* consumerSettings.closeTimeout */)
      }
    } catch (e: Throwable) {
      // ConsumerEventLoop.log.warn("Cancel exception: $e")
    }
  }
  
  inner class PollEvent : suspend () -> Unit {
    private val pollTimeout: Duration = consumerSettings.maxPollInterval.toKotlinDuration()
    private val maxDeferredCommits: Long = consumerSettings.maxPollRecords.toLong()
    private val pausedByUs = AtomicBoolean()
    private val pausedByUser: MutableSet<TopicPartition> = HashSet()
    private val scheduled = AtomicBoolean()
    private val commitBatch: CommittableBatch = commitEvent.commitBatch
    
    override suspend fun invoke() {
      try {
        scheduled.set(false)
        if (isActive.get()) {
          // Ensure that commits are not queued behind polls since number of poll events is chosen by reactor.
          commitEvent.runIfRequired(false)
          // val pauseForDeferred = (maxDeferredCommits > 0 && commitBatch.deferredCount() >= maxDeferredCommits)
          // if (pauseForDeferred || commitEvent.retrying.get()) {
          //   r = 0
          // }
          // if (r > 0) {
            if (!awaitingTransaction.get()) {
              if (pausedByUs.getAndSet(false)) {
                val toResume: MutableSet<TopicPartition> = java.util.HashSet(consumer.assignment())
                toResume.removeAll(pausedByUser)
                pausedByUser.clear()
                consumer.resume(toResume)
                // ConsumerEventLoop.log.debug("Resumed")
              }
            } else {
              if (checkAndSetPausedByUs()) {
                pausedByUser.addAll(consumer.paused())
                consumer.pause(consumer.assignment())
                // ConsumerEventLoop.log.debug("Paused - awaiting transaction")
              }
            }
          // } else if (checkAndSetPausedByUs()) {
          //   pausedByUser.addAll(consumer.paused())
          //   consumer.pause(consumer.assignment())
          //   if (pauseForDeferred) {
          //     ConsumerEventLoop.log.debug("Paused - too many deferred commits")
            // } else if (commitEvent.retrying.get()) {
            //   ConsumerEventLoop.log.debug("Paused - commits are retrying")
            // } else {
            //   ConsumerEventLoop.log.debug("Paused - back pressure")
            // }
          // }
          var records: ConsumerRecords<K, V>
          try {
            // Make interruption-safe
            records = consumer.poll(pollTimeout.toJavaDuration())
          } catch (e: WakeupException) {
            // ConsumerEventLoop.log.debug("Consumer woken")
            records = ConsumerRecords.empty()
          }
          if (isActive.get()) {
            schedule()
          }
          if (!records.isEmpty) {
            if (maxDeferredCommits > 0) {
              commitBatch.addUncommitted(records)
            }
            // produced(REQUESTED, this@ConsumerEventLoop, 1)
            // ConsumerEventLoop.log.debug("Emitting ${records.count()} records, requested now $r")
            // sink.emitNext(records, this@ConsumerEventLoop)
            // collector.send(records)
          }
        }
      } catch (e: java.lang.Exception) {
        if (isActive.get()) {
          // ConsumerEventLoop.log.error("Unexpected exception", e)
          // sink.emitError(e, this@ConsumerEventLoop)
          // throw e
          collector.close(e)
        }
      }
    }
    
    /*
     * Race condition where onRequest was called to increase requested but we
     * hadn't yet paused the consumer; wake immediately in this case.
     */
    private fun checkAndSetPausedByUs(): Boolean {
      val pausedNow = !pausedByUs.getAndSet(true)
      if (pausedNow /*&& requested > 0L*/ && !commitEvent.retrying.get()) consumer.wakeup()
      return pausedNow
    }
    
    suspend fun schedule(): Unit =
      if (!scheduled.getAndSet(true)) invoke() else Unit
  
    fun isPaused(): Boolean =
      pausedByUs.get()
  }
  
  inner class CommitEvent : suspend () -> Unit {
    val commitBatch: CommittableBatch = CommittableBatch()
    private val isPending = AtomicBoolean()
    private val inProgress = AtomicInteger()
    private val consecutiveCommitFailures = AtomicInteger()
    val retrying = AtomicBoolean()
    private val pollEvent = PollEvent()
    
    override suspend fun invoke() {
      if (!isPending.compareAndSet(true, false)) return
      val commitArgs: CommittableBatch.CommitArgs = commitBatch.getAndClearOffsets()
      try {
        if (commitArgs.offsets.isEmpty()) handleSuccess(commitArgs, commitArgs.offsets)
        else {
          when (ackMode) {
            AckMode.MANUAL_ACK -> {
              inProgress.incrementAndGet()
              try {
                // if (ConsumerEventLoop.log.isDebugEnabled()) {
                //   ConsumerEventLoop.log.debug("Async committing: " + commitArgs.offsets())
                // }
                consumer.commitAsync(commitArgs.offsets) { offsets, exception ->
                  inProgress.decrementAndGet()
                  if (exception == null) handleSuccess(commitArgs, offsets)
                  else handleFailure(commitArgs, exception)
                }
              } catch (e: Throwable) {
                inProgress.decrementAndGet()
                throw e
              }
              pollEvent.schedule()
            }
          }
        }
      } catch (e: Exception) {
        // ConsumerEventLoop.log.error("Unexpected exception", e)
        handleFailure(commitArgs, e)
      }
    }
    
    suspend fun runIfRequired(force: Boolean) {
      if (force) isPending.set(true)
      if (!retrying.get() && isPending.get()) invoke()
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
    
    private fun handleFailure(commitArgs: CommittableBatch.CommitArgs, exception: Exception) {
      // ConsumerEventLoop.log.warn("Commit failed", exception)
      val mayRetry = isRetriableException(exception) &&               // TODO maxCommitAttempts
        consecutiveCommitFailures.incrementAndGet() < consumerSettings.maxPollRecords
      
      if (!mayRetry) {
        // ConsumerEventLoop.log.debug("Cannot retry")
        pollTaskAfterRetry()
        val callbackEmitters: List<Continuation<Unit>>? = commitArgs.continuations
        if (!callbackEmitters.isNullOrEmpty()) {
          isPending.set(false)
          commitBatch.restoreOffsets(commitArgs, false)
          callbackEmitters.forEach { continuation: Continuation<Unit> ->
            continuation.resumeWithException(exception)
          }
        } else {
          // throw exception
          //sink.emitError(exception, this@ConsumerEventLoop)
          collector.close(exception)
        }
      } else {
        commitBatch.restoreOffsets(commitArgs, true)
        // ConsumerEventLoop.log.warn(                                      // TODO maxCommitAttempts
        //   "Commit failed with exception $exception, retries remaining ${(consumerSettings.maxPollRecords - consecutiveCommitFailures.get())}"
        // )
        isPending.set(true)
        retrying.set(true)
        launch { pollEvent.schedule() }
        
        launch { // TODO schedule delayed task ??
          // delay(consumerSettings.commitRetryInterval) TODO
          invoke()
        }
      }
    }
    
    private fun pollTaskAfterRetry() {
      // if (ConsumerEventLoop.log.isTraceEnabled()) {
      //   ConsumerEventLoop.log.trace("after retry " + retrying.get())
      // }
      if (retrying.getAndSet(false)) {
        launch { pollEvent.schedule() }
      }
    }
    
    fun scheduleIfRequired() {
      if (isActive.get() && !retrying.get() && isPending.compareAndSet(false, true)) {
        launch { invoke() }
      }
    }
    
    fun waitFor(endTimeMillis: Long) {
      while (inProgress.get() > 0 && endTimeMillis - System.currentTimeMillis() > 0) {
        consumer.poll(java.time.Duration.ofMillis(1))
      }
    }
  }
  
  suspend fun close(timeout: Duration): Unit =
    CloseEvent(timeout)()
  
  inner class CloseEvent constructor(timeout: Duration) : suspend () -> Unit {
    private val closeEndTimeMillis: Long = System.currentTimeMillis() + timeout.inWholeMilliseconds
    
    // TODO Complete code
    override suspend fun invoke() {
      try {
        // val manualAssignment: Collection<TopicPartition> = consumerSettings.assignment()
        // if (manualAssignment.isNotEmpty()) onPartitionsRevoked(manualAssignment)
        /*
         * We loop here in case the consumer has had a recent wakeup call (from user code)
         * which will cause a poll() (in waitFor) to be interrupted while we're
         * possibly waiting for async commit results.
         */
        val attempts = 3
        for (i in 0 until attempts) {
          try {
            var forceCommit = true
            // if (ackMode == AckMode.ATMOST_ONCE) {
            //   forceCommit = atmostOnceOffsets.undoCommitAhead(commitEvent.commitBatch)
            // }
            // For exactly-once, offsets are committed by a producer, consumer may be closed immediately
            // if (ackMode != AckMode.EXACTLY_ONCE) {
            //   commitEvent.runIfRequired(forceCommit)
            //   commitEvent.waitFor(closeEndTimeMillis)
            // }
            var timeoutMillis = closeEndTimeMillis - System.currentTimeMillis()
            if (timeoutMillis < 0) timeoutMillis = 0
            consumer.close(java.time.Duration.ofMillis(timeoutMillis))
            break
          } catch (e: WakeupException) {
            if (i == attempts - 1) throw e
          }
        }
      } catch (e: java.lang.Exception) {
        // ConsumerEventLoop.log.error("Unexpected exception during close", e)
        // sink.emitError(e, this@ConsumerEventLoop)
        // throw e
        collector.close(e)
      }
    }
  }
}
