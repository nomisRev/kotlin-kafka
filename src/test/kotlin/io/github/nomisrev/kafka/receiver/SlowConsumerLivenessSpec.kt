package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.await
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverRecord
import io.github.nomisrev.kafka.KafkaSpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.withIndex
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import java.time.Duration as JavaDuration
import java.util.Properties
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.times

/**
 * A collector that suspends on a record for longer than `max.poll.interval.ms` must not get the
 * consumer kicked out of its group. While downstream is back pressured the event loop has to keep
 * polling, with every partition paused, rather than stop polling until downstream catches up.
 */
class SlowConsumerLivenessSpec : KafkaSpec() {

  private val maxPollInterval = 2.seconds
  private val processingDelay = 4 * maxPollInterval

  private fun slowConsumerSettings(groupId: String) =
    receiverSetting().copy(
      groupId = groupId,
      pollTimeout = 100.milliseconds,
      properties = Properties().apply {
        put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval.inWholeMilliseconds.toString())
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10")
      }
    )

  private suspend fun memberIds(groupId: String): List<String> = admin {
    describeConsumerGroups(listOf(groupId)).describedGroups().getValue(groupId).await()
      .members().map { it.consumerId() }
  }

  /* Control: a plain KafkaConsumer that blocks past max.poll.interval.ms loses its membership,
   * which shows the configuration above is strict enough to catch the problem. */
  @Test
  fun `blocking a plain KafkaConsumer past max poll interval loses the group membership`() =
    withTopic(partitions = 1) {
      publishToKafka((0 until 200).map { createProducerRecord(it, partitions = 1) })
      val groupId = "liveness-control-${UUID.randomUUID()}"
      val properties = slowConsumerSettings(groupId).toProperties()
      withContext(Dispatchers.IO) {
        KafkaConsumer(properties, StringDeserializer(), StringDeserializer()).use { consumer ->
          consumer.subscribe(listOf(topic.name()))
          while (consumer.poll(JavaDuration.ofMillis(100)).isEmpty) Unit
          val before = memberIds(groupId)
          assertEquals(1, before.size)

          Thread.sleep(processingDelay.inWholeMilliseconds)

          val during = memberIds(groupId)
          assertTrue(during.isEmpty(), "Expected the blocked consumer to have left the group, but found $during")
        }
      }
      // Drain the topic, so the spec's emptiness check passes
      KafkaReceiver().receive(topic.name()).collectN(200)
    }

  @Test
  fun `suspending the collector past max poll interval keeps the group membership`() =
    suspendPastMaxPollInterval(records = 200)

  /* Few enough records that the buffers downstream of the event loop never fill up. */
  @Test
  fun `suspending the collector past max poll interval without a backlog keeps the group membership`() =
    suspendPastMaxPollInterval(records = 5)

  private fun suspendPastMaxPollInterval(records: Int) =
    withTopic(partitions = 1) {
      publishToKafka((0 until records).map { createProducerRecord(it, partitions = 1) })
      val groupId = "liveness-${UUID.randomUUID()}"
      withContext(Dispatchers.Default) {
        val received = KafkaReceiver(slowConsumerSettings(groupId))
          .receive(topic.name())
          .take(records)
          .withIndex()
          .map { (index, value) ->
            val record = value
            if (index == 0) {
              val before = memberIds(groupId)
              assertEquals(1, before.size)

              // Long enough for the event loop to hit back pressure on the records behind this one
              delay(processingDelay)

              val during = memberIds(groupId)
              assertEquals(before, during, "Consumer lost its group membership while the collector was suspended")
            }
            record.offset.acknowledge()
            record.value()
          }.toList()

        assertEquals((0 until records).map { "Message $it" }, received)
      }
      // Drain the topic, so the spec's emptiness check passes
      KafkaReceiver().receive(topic.name()).collectN(records)
    }

  private suspend fun <K, V> Flow<ReceiverRecord<K, V>>.collectN(n: Int): Unit =
    take(n).collect { it.offset.acknowledge() }
}
