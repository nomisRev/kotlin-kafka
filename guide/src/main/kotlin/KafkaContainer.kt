import org.testcontainers.kafka.ConfluentKafkaContainer
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
class Kafka : ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.12")) {

  companion object {
    val container: ConfluentKafkaContainer by lazy {
      Kafka().also { it.start() }
    }
  }
}
