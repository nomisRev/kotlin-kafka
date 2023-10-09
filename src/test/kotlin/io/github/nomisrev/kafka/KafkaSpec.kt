package io.github.nomisrev.kafka

import io.github.nomisRev.kafka.Admin
import io.github.nomisRev.kafka.AdminSettings
import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.github.nomisRev.kafka.createTopic
import io.github.nomisRev.kafka.deleteTopic
import io.github.nomisRev.kafka.describeTopic
import io.github.nomisRev.kafka.publisher.Acks
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.publisher.PublisherSettings
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.kotest.assertions.assertSoftly
import io.kotest.assertions.async.shouldTimeout
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

abstract class KafkaSpec(body: KafkaSpec.() -> Unit = {}) : StringSpec() {
  init {
    body()
  }

  private val transactionTimeoutInterval = 1.seconds
  private val consumerPollingTimeout = 1.seconds
  val boom = RuntimeException("Boom!")

  private val kafkaImage: DockerImageName =
    DockerImageName.parse("confluentinc/cp-kafka:latest")

  private val container: KafkaContainer = autoClose(
    KafkaContainer(kafkaImage)
      .withExposedPorts(9092, 9093)
      .withNetworkAliases("broker")
      .withEnv("KAFKA_HOST_NAME", "broker")
      .withEnv("KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR", "1")
      .withEnv("KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR", "1")
      .withEnv(
        "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS",
        transactionTimeoutInterval.inWholeMilliseconds.toString()
      )
      .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
      .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true")
      .withReuse(true)
      .also { container -> container.start() }
  )

  private fun adminProperties(): Properties = Properties().apply {
    put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers)
    put(AdminClientConfig.CLIENT_ID_CONFIG, "test-kafka-admin-client")
    put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "10000")
  }

  fun adminSettings(): AdminSettings =
    AdminSettings(container.bootstrapServers, adminProperties())

  inline fun <A> admin(body: Admin.() -> A): A =
    Admin(adminSettings()).use(body)

  fun receiverSetting(): ReceiverSettings<String, String> =
    ReceiverSettings(
      bootstrapServers = container.bootstrapServers,
      keyDeserializer = StringDeserializer(),
      valueDeserializer = StringDeserializer(),
      groupId = "test-group-id",
      autoOffsetReset = AutoOffsetReset.Earliest,
      pollTimeout = consumerPollingTimeout
    )

  fun publisherSettings(
    acknowledgments: Acks = Acks.One,
    properties: Properties = Properties()
  ): PublisherSettings<String, String> =
    PublisherSettings(
      bootstrapServers = container.bootstrapServers,
      keySerializer = StringSerializer(),
      valueSerializer = StringSerializer(),
      acknowledgments = acknowledgments,
      properties = properties.apply {
        put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000.toString())
        put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000.toString())
      }
    )

  val producer = KafkaProducer<String, String>(publisherSettings().properties())
  val publisher = autoClose(KafkaPublisher(publisherSettings()) { producer })

  private fun nextTopicName(): String =
    "topic-${UUID.randomUUID()}"

  suspend fun <A> withTopic(
    topicConfig: Map<String, String> = emptyMap(),
    partitions: Int = 4,
    replicationFactor: Short = 1,
    action: suspend Admin.(NewTopic) -> A,
  ): A {
    val topic = NewTopic(nextTopicName(), partitions, replicationFactor).configs(topicConfig)
    return admin {
      createTopic(topic)
      try {
        action(topic)
      } finally {
        topic.shouldBeEmpty()
        deleteTopic(topic.name())
      }
    }
  }

  @JvmName("publishPairsToKafka")
  suspend fun publishToKafka(
    topic: NewTopic,
    messages: Iterable<Pair<String, String>>,
  ): Unit =
    publishToKafka(messages.map { (key, value) ->
      ProducerRecord(topic.name(), key, value)
    })

  suspend fun publishToKafka(messages: Iterable<ProducerRecord<String, String>>): Unit =
    publisher.publishScope {
      offer(messages)
    }

  suspend fun <K, V> KafkaReceiver<K, V>.committedCount(topic: String): Long =
    admin {
      val description = requireNotNull(describeTopic(topic)) { "Topic $topic not found" }
      val topicPartitions = description.partitions().map {
        TopicPartition(topic, it.partition())
      }.toSet()

      withConsumer {
        committed(topicPartitions)
          .mapNotNull { (_, offset) ->
            offset?.takeIf { it.offset() > 0 }?.offset()
          }.sum()
      }
    }

  @JvmName("shouldHaveAllRecords")
  suspend fun NewTopic.shouldHaveRecords(
    records: Iterable<Iterable<ProducerRecord<String, String>>>
  ) {
    val expected =
      records.flatten().groupBy({ it.partition() }) { it.value() }.mapValues { it.value.toSet() }
    KafkaReceiver(receiverSetting())
      .receive(name())
      .map { record ->
        record.also { record.offset.acknowledge() }
      }
      .take(records.flatten().size)
      .toList()
      .groupBy({ it.partition() }) { it.value() }
      .mapValues { it.value.toSet() } shouldBe expected
  }

  suspend fun NewTopic.shouldHaveRecords(records: Iterable<ProducerRecord<String, String>>) {
    KafkaReceiver(receiverSetting())
      .receive(name())
      .map { record ->
        record
          .also { record.offset.acknowledge() }
      }
      .take(records.toList().size)
      .toList()
      .groupBy({ it.partition() }) { it.value() } shouldBe records.groupBy({ it.partition() }) { it.value() }
  }

  suspend fun NewTopic.shouldBeEmpty() {
    shouldTimeout(100.milliseconds) {
      KafkaReceiver(receiverSetting())
        .receive(name())
        .take(1)
        .toList()
    }
  }

  suspend fun NewTopic.shouldHaveRecord(records: ProducerRecord<String, String>) {
    assertSoftly {
      KafkaReceiver(receiverSetting())
        .receive(name())
        .map {
          it.apply { offset.acknowledge() }
        }.take(1)
        .map { it.value() }
        .toList() shouldBe listOf(records.value())
      shouldBeEmpty()
    }
  }

  suspend fun topicWithSingleMessage(topic: NewTopic, record: ProducerRecord<String, String>) =
    KafkaReceiver(receiverSetting())
      .receive(topic.name())
      .map {
        it.apply { offset.acknowledge() }
      }.first().value() shouldBe record.value()

  suspend fun topicShouldBeEmpty(topic: NewTopic) =
    shouldTimeout(1.seconds) {
      KafkaReceiver(receiverSetting())
        .receive(topic.name())
        .take(1)
        .toList()
    }

  fun stubProducer(failOnNumber: Int? = null): suspend () -> Producer<String, String> = suspend {
    object : Producer<String, String> {
      override fun close() {}

      override fun close(timeout: Duration?) {}

      override fun initTransactions() =
        producer.initTransactions()

      override fun beginTransaction() =
        producer.beginTransaction()

      @Suppress("DEPRECATION")
      @Deprecated("Deprecated in Java")
      override fun sendOffsetsToTransaction(
        offsets: MutableMap<TopicPartition, OffsetAndMetadata>?,
        consumerGroupId: String?
      ) = producer.sendOffsetsToTransaction(offsets, consumerGroupId)

      override fun sendOffsetsToTransaction(
        offsets: MutableMap<TopicPartition, OffsetAndMetadata>?,
        groupMetadata: ConsumerGroupMetadata?
      ) = producer.sendOffsetsToTransaction(offsets, groupMetadata)

      override fun commitTransaction() =
        producer.commitTransaction()

      override fun abortTransaction() =
        producer.abortTransaction()

      override fun flush() =
        producer.flush()

      override fun partitionsFor(topic: String?): MutableList<PartitionInfo> =
        producer.partitionsFor(topic)

      override fun metrics(): MutableMap<MetricName, out Metric> =
        producer.metrics()

      override fun send(record: ProducerRecord<String, String>, callback: Callback): Future<RecordMetadata> =
        if (failOnNumber != null && record.key() == failOnNumber.toString()) {
          Executors.newScheduledThreadPool(1).schedule(
            {
              callback.onCompletion(null, boom)
            },
            50,
            TimeUnit.MILLISECONDS
          )

          CompletableFuture.supplyAsync { throw AssertionError("Should never be called") }
        } else producer.send(record, callback)

      override fun send(record: ProducerRecord<String, String>): Future<RecordMetadata> =
        producer.send(record)
    }
  }
}
