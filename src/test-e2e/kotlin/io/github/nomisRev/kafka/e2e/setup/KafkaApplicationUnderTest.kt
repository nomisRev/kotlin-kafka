package io.github.nomisRev.kafka.e2e.setup

import com.trendyol.stove.testing.e2e.system.abstractions.ApplicationUnderTest
import io.github.nomisRev.kafka.e2e.setup.example.KafkaTestShared
import io.github.nomisRev.kafka.e2e.setup.example.ReceiveMethod
import io.github.nomisRev.kafka.e2e.setup.example.StoveKafkaValueDeserializer
import io.github.nomisRev.kafka.e2e.setup.example.StoveKafkaValueSerializer
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.publisher.PublisherSettings
import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import kotlin.time.Duration.Companion.seconds

/**
 * Stove's Kafka application under test implementation
 */
class KafkaApplicationUnderTest : ApplicationUnderTest<Unit> {
  private lateinit var client: AdminClient
  private val consumers: MutableList<AutoCloseable> = mutableListOf()

  override suspend fun start(configurations: List<String>) {
    val bootstrapServers = configurations.first { it.contains("kafka.servers", true) }.split('=')[1]
    val interceptorClass = configurations.first { it.contains("kafka.interceptor-classes", true) }.split('=')[1]
    val receiveMethod = configurations.first { it.contains("kafka.receive-method", true) }.split('=')[1]
    client = createAdminClient(bootstrapServers)
    createTopics(client)
    startConsumers(bootstrapServers, interceptorClass, ReceiveMethod.from(receiveMethod))
  }

  private fun createAdminClient(bootstrapServers: String): AdminClient {
    return mapOf<String, Any>(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers
    ).let { AdminClient.create(it) }
  }

  private fun createTopics(client: AdminClient) {
    val newTopics = KafkaTestShared.topics.flatMap {
      listOf(it.topic, it.retryTopic, it.deadLetterTopic)
    }.map { NewTopic(it, 1, 1) }
    client.createTopics(newTopics).all().get()
  }

  private fun startConsumers(bootStrapServers: String, interceptorClass: String, receiveMethod: ReceiveMethod) {
    val (publisher, receiver) = createPublisherAndReceiver(interceptorClass, bootStrapServers)
    val configuredConsumers = KafkaTestShared.consumers(receiver, publisher, receiveMethod)
    configuredConsumers.forEach { it.start() }
    consumers.addAll(configuredConsumers)
  }

  private fun createPublisherAndReceiver(
    interceptorClass: String, bootStrapServers: String
  ): Pair<KafkaPublisher<String, Any>, KafkaReceiver<String, Any>> {
    val consumerSettings = mapOf(
      ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG to "2000",
      ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG to "true", // Expected to be created by the client
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
      ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG to listOf(interceptorClass)
    )

    val receiverSettings = ReceiverSettings(bootstrapServers = bootStrapServers,
      valueDeserializer = StoveKafkaValueDeserializer(),
      keyDeserializer = StringDeserializer(),
      groupId = "stove-application-consumers",
      commitStrategy = CommitStrategy.ByTime(2.seconds),
      pollTimeout = 1.seconds,
      properties = Properties().apply {
        putAll(consumerSettings)
      })

    val producerSettings = PublisherSettings<String, Any>(bootStrapServers,
      StringSerializer(),
      StoveKafkaValueSerializer(),
      properties = Properties().apply {
        put(
          ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, listOf(interceptorClass)
        )
      })

    val publisher = KafkaPublisher(producerSettings)
    val receiver = KafkaReceiver(receiverSettings)
    return Pair(publisher, receiver)
  }

  override suspend fun stop() {
    client.close()
    consumers.forEach { it.close() }
  }
}
