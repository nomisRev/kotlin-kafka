import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.computations.resource
import arrow.fx.coroutines.fromAutoCloseable
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runInterruptible
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

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
