package io.github.nomisrev.kafka

import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName

class Kafka : ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.12")) {

  fun pause() {
    dockerClient.pauseContainerCmd(containerId).exec()
  }

  fun unpause() {
    dockerClient.unpauseContainerCmd(containerId).exec()
  }
}
