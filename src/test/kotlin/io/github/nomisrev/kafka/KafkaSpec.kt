package io.github.nomisrev.kafka

import io.github.nomisRev.kafka.Admin
import io.github.nomisRev.kafka.AdminSettings
import io.github.nomisRev.kafka.AutoOffsetReset
import io.github.nomisRev.kafka.ConsumerSettings
import io.github.nomisRev.kafka.NothingDeserializer.close
import io.github.nomisRev.kafka.ProducerSettings
import io.github.nomisRev.kafka.createTopic
import io.github.nomisRev.kafka.produce
import io.kotest.core.spec.style.StringSpec
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.Properties
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

abstract class KafkaSpec(body: KafkaSpec.() -> Unit = {}) : StringSpec() {
  init {
    body()
  }
  
  val adminClientCloseTimeout = 2.seconds
  val transactionTimeoutInterval = 1.seconds
  val consumerPollingTimeout = 1.seconds
  val producerPublishTimeout = 10.seconds
  
  private val imageVersion = "7.0.1"
  
  private val kafkaImage: DockerImageName =
    if (System.getProperty("os.arch") == "aarch64") DockerImageName.parse("niciqy/cp-kafka-arm64:$imageVersion")
      .asCompatibleSubstituteFor("confluentinc/cp-kafka")
    else DockerImageName.parse("confluentinc/cp-kafka:$imageVersion")
  
  val container: KafkaContainer = autoClose(
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
      .also { container -> container.start() }
  )
  
  fun adminProperties(): Properties = Properties().apply {
    put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers)
    put(AdminClientConfig.CLIENT_ID_CONFIG, "test-kafka-admin-client")
    put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "10000")
  }
  
  fun adminSettings(): AdminSettings =
    AdminSettings(container.bootstrapServers, adminProperties())
  
  inline fun <A> admin(
    body: Admin.() -> A,
  ): A = Admin(adminSettings()).use(body)
  
  fun consumerProperties(): Properties = Properties().apply {
    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers)
    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-id")
    put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  }
  
  fun consumerSetting(): ConsumerSettings<String, String> =
    ConsumerSettings(
      bootstrapServers = container.bootstrapServers,
      keyDeserializer = StringDeserializer(),
      valueDeserializer = StringDeserializer(),
      groupId = "test-group-id",
      autoOffsetReset = AutoOffsetReset.Earliest,
      enableAutoCommit = false
    )
  
  fun producerProperties(): Properties = Properties().apply {
    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers)
    put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000.toString())
    put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000.toString())
  }
  
  fun producerSettings(): ProducerSettings<String, String> =
    ProducerSettings(
      bootstrapServers = container.bootstrapServers,
      keyDeserializer = StringSerializer(),
      valueDeserializer = StringSerializer(),
      other = producerProperties()
    )
  
  private fun nextTopicName(): String =
    "topic-${UUID.randomUUID()}"
  
  suspend fun createCustomTopic(
    topic: String = nextTopicName(),
    topicConfig: Map<String, String> = emptyMap(),
    partitions: Int = 1,
    replicationFactor: Short = 1,
  ): NewTopic =
    NewTopic(topic, partitions, replicationFactor)
      .configs(topicConfig)
      .also { topic ->
        admin { createTopic(topic) }
      }
  
  fun publishToKafka(
    topic: NewTopic,
    message: String,
  ): Unit = TODO()
  
  suspend fun publishToKafka(
    topic: NewTopic,
    messages: Iterable<Pair<String, String>>,
  ): Unit =
    messages.asFlow()
      .map { (key, value) ->
        ProducerRecord(topic.name(), key, value)
      }.produce(producerSettings())
      .collect()
  
  @ExperimentalCoroutinesApi
  fun <T> Flow<T>.takeUntil(notifier: suspend () -> Unit): Flow<T> = channelFlow {
    val outerScope = this
    
    launch {
      try {
        notifier()
        close()
      } catch (e: CancellationException) {
        outerScope.cancel(e) // cancel outer scope on cancellation exception, too
      }
    }
    launch {
      try {
        collect { send(it) }
        close()
      } catch (e: CancellationException) {
        outerScope.cancel(e) // cancel outer scope on cancellation exception, too
      }
    }
  }
}
