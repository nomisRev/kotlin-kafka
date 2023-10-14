package io.github.nomisrev.kafka

import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

class Kafka : KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest")) {

  fun pause() {
    dockerClient.pauseContainerCmd(containerId).exec()
  }

  fun unpause() {
    dockerClient.unpauseContainerCmd(containerId).exec()
  }
}
