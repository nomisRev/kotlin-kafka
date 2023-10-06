package io.github.nomisRev.kafka.receiver

import io.github.nomisRev.kafka.receiver.internals.PollLoop
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

// TODO Copied name from reactor-kafka,
//  conflict with org.apache.kafka.clients.consumer.KafkaConsumer,
//  or figure out a other good name
public interface KafkaReceiver<K, V> {

  public fun receive(topicNames: Collection<String>): Flow<ReceiverRecord<K, V>>

  public fun receive(topicName: String): Flow<ReceiverRecord<K, V>> =
    receive(setOf(topicName))

  public fun receiveAutoAck(topicNames: Collection<String>): Flow<Flow<ConsumerRecord<K, V>>>

  public fun receiveAutoAck(topicNames: String): Flow<Flow<ConsumerRecord<K, V>>> =
    receiveAutoAck(setOf(topicNames))

  public suspend fun <A> withConsumer(action: suspend KafkaConsumer<K, V>.(KafkaConsumer<K, V>) -> A): A
}

public fun <K, V> KafkaReceiver(settings: ReceiverSettings<K, V>): KafkaReceiver<K, V> =
  DefaultKafkaReceiver(settings)

private class DefaultKafkaReceiver<K, V>(private val settings: ReceiverSettings<K, V>) : KafkaReceiver<K, V> {

  override suspend fun <A> withConsumer(action: suspend KafkaConsumer<K, V>.(KafkaConsumer<K, V>) -> A): A =
    KafkaConsumer(settings.toProperties(), settings.keyDeserializer, settings.valueDeserializer)
      .use { action(it, it) }

  @OptIn(ExperimentalCoroutinesApi::class)
  override fun receive(topicNames: Collection<String>): Flow<ReceiverRecord<K, V>> =
    scopedConsumer(settings.groupId) { scope, dispatcher, consumer ->
      val loop = PollLoop(topicNames, settings, consumer, scope)
      loop.receive().flowOn(dispatcher)
        .flatMapConcat { records ->
          records.map { record ->
            ReceiverRecord(record, loop.toCommittableOffset(record))
          }.asFlow()
        }
    }

  override fun receiveAutoAck(topicNames: Collection<String>): Flow<Flow<ConsumerRecord<K, V>>> =
    scopedConsumer(settings.groupId) { scope, dispatcher, consumer ->
      val loop = PollLoop(topicNames, settings, consumer, scope)
      loop.receive().flowOn(dispatcher).map { records ->
        records.asFlow()
          .onCompletion { records.forEach { loop.toCommittableOffset(it).acknowledge() } }
      }
    }

  private val logger: Logger =
    LoggerFactory.getLogger("KafkaScheduler")

  // All exceptions inside the library code should be handled.
  // So any uncaught errors on the KafkaConsumer dispatcher is a bug.
  private val defaultCoroutineExceptionHandler = CoroutineExceptionHandler { coroutineContext, throwable ->
    logger.error(
      "KafkaDispatcher with $coroutineContext failed with an uncaught exception. Report to kotlin-kafka repo please.",
      throwable
    )
  }

  /**
   * [org.apache.kafka.clients.consumer.KafkaConsumer] is single-threaded,
   * and thus needs to have a dedicated [ExecutorCoroutineDispatcher] that guarantees a single thread
   * where we can schedule our interactions through [org.apache.kafka.clients.consumer.KafkaConsumer].
   *
   * This [Flow] returns a scheduler and CoroutineScope that is scoped to the stream,
   * it gets lazily initialized when the [Flow] is collected and gets closed when the flow terminates.
   */
  @OptIn(ExperimentalCoroutinesApi::class)
  fun <A> scopedConsumer(
    groupId: String,
    block: (CoroutineScope, ExecutorCoroutineDispatcher, KafkaConsumer<K, V>) -> Flow<A>
  ): Flow<A> = flow {
    kafkaConsumerDispatcher(groupId).use { dispatcher: ExecutorCoroutineDispatcher ->
      val job = Job()
      val scope = CoroutineScope(job + dispatcher + defaultCoroutineExceptionHandler)
      try {
        KafkaConsumer(settings.toProperties(), settings.keyDeserializer, settings.valueDeserializer)
          .use { emit(block(scope, dispatcher, it)) }
      } finally {
        job.cancelAndJoin()
      }
    }
  }.flattenConcat()

  private fun kafkaConsumerDispatcher(groupId: String): ExecutorCoroutineDispatcher =
    ScheduledThreadPoolExecutor(1, EventThreadFactory(groupId)).apply {
      removeOnCancelPolicy = true
      maximumPoolSize = 1
    }.asCoroutineDispatcher()


  // Custom [ThreadFactory] to give a more meaningful name: "kotlin-kafka-groupId-counter"
  private class EventThreadFactory(private val groupId: String) : ThreadFactory {
    override fun newThread(runnable: java.lang.Runnable): Thread =
      Thread(runnable, "$PREFIX$groupId-${COUNTER_REFERENCE.incrementAndGet()}")
  }
}

private const val PREFIX = "kotlin-kafka-"
private val COUNTER_REFERENCE = AtomicLong()
