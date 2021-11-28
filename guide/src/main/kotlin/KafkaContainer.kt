import com.github.dockerjava.api.command.InspectContainerResponse
import com.github.nomisRev.kafka.Admin
import com.github.nomisRev.kafka.AdminSettings
import com.github.nomisRev.kafka.await
import kotlinx.coroutines.runBlocking
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * A singleton `Kafka` Test Container.
 *
 * This setup guarantees that the container is `reuseable` **if** you have the following setting: In
 * `~/.testcontainers.properties` you need to add following line: `testcontainers.reuse.enable=true`
 *
 * With this flag enabled, test containers will now be able to re-use existing containers, which
 * save about 10s per container of start-up. This container starts in ~3s when being re-used, and
 * that only happens once per project.
 *
 * There is no need to `close` or `stop` the test-container since the lifecycle is now 100%
 * controlled by TC.
 *
 * ```kotlin
 * class MySpec : StringSpec({
 *   val kafka = Kafka.container
 *   ...
 * })
 * ```
 *
 * @see https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/
 * @see https://pawelpluta.com/optimise-testcontainers-for-better-tests-performance/
 */
class Kafka private constructor(imageName: DockerImageName) : KafkaContainer(imageName) {

  companion object {
    val container: KafkaContainer by lazy {
      Kafka(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
        .withReuse(true)
        .withNetwork(null)
        .withLabel("io.github.nomisrev.kafka", "fqn-testcontainers-reuse")
        .also { it.start() }
    }
  }

  override fun containerIsStarted(containerInfo: InspectContainerResponse?, reused: Boolean) {
    super.containerIsStarted(containerInfo, reused)
    // If we're reusing the container, we want to reset the state of the container. We do this by
    // deleting all topics.
    //    if (reused)
    runBlocking<Unit> {
      Admin(AdminSettings(bootstrapServers)).use { admin ->
        val names = admin.listTopics().listings().await()
        admin.deleteTopics(names.map { it.name() }).all().await()
      }
    }
  }
}