import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.computations.resource
import arrow.fx.coroutines.fromAutoCloseable
import java.util.Properties
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runInterruptible
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * Launches kafka test container, creates the Properties based on the TC configuration.
 * And creates an AdminClient for the Test Container configuration
 */
fun launchKafka(): Resource<Pair<Properties, AdminClient>> = resource {
  val kafka = kafkaContainer().bind()
  val props = exampleProps(kafka.bootstrapServers)
  Pair(props, adminClient(props).bind())
}

/** Construct a AdminClient as a Resource */
fun adminClient(properties: Properties): Resource<AdminClient> =
  Resource.fromAutoCloseable { AdminClient.create(properties) }

/**
 * Construct, and start a KafkaContainer as a Resource.
 * [KafkaContainer.start] is called in a cancellable way here.
 */
fun kafkaContainer(fullImageName: String = "confluentinc/cp-kafka:6.2.1"): Resource<KafkaContainer> =
  resource {
    val kafka = Resource.fromAutoCloseable {
      KafkaContainer(DockerImageName.parse(fullImageName))
    }.bind()
    // start is an interruptible blocking Java method
    runInterruptible(Dispatchers.IO) { kafka.start() }
    kafka
  }

// Constructs Properties with default settings for our example
fun exampleProps(bootstrapServers: String) = Properties().apply {
  put(ProducerConfig.ACKS_CONFIG, "all")
  put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
  put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
  put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, StringSerializer::class.qualifiedName)

  put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  put(ConsumerConfig.GROUP_ID_CONFIG, "kotlin_example_group_1")
  put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
}
