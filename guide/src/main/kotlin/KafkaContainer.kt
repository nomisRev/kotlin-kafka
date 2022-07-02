import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.lang.System.getProperty

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
    private val image: DockerImageName =
      if (getProperty("os.arch") == "aarch64") DockerImageName.parse("niciqy/cp-kafka-arm64:7.0.1")
        .asCompatibleSubstituteFor("confluentinc/cp-kafka")
      else DockerImageName.parse("confluentinc/cp-kafka:6.2.1")
    
    val container: KafkaContainer by lazy {
      Kafka(image).also { it.start() }
    }
  }
  
  // override fun containerIsStarted(containerInfo: InspectContainerResponse?, reused: Boolean) {
  //   super.containerIsStarted(containerInfo, reused)
  //   // If we're reusing the container, we want to reset the state of the container. We do this by
  //   // deleting all topics.
  //   //    if (reused)
  //   runBlocking<Unit> {
  //     Admin(AdminSettings(bootstrapServers)).use { admin ->
  //       val names = admin.listTopics().listings().await()
  //       admin.deleteTopics(names.map { it.name() }).all().await()
  //     }
  //   }
  // }
}
