package io.github.nomisrev.kafka

import io.kotest.core.TestConfiguration
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

private val kafkaImage: DockerImageName =
  if (System.getProperty("os.arch") == "aarch64") DockerImageName.parse("niciqy/cp-kafka-arm64:7.0.1")
    .asCompatibleSubstituteFor("confluentinc/cp-kafka")
  else DockerImageName.parse("confluentinc/cp-kafka:6.2.1")

fun TestConfiguration.kafkaContainer(): KafkaContainer =
  autoClose(
    KafkaContainer(kafkaImage)
      .withExposedPorts(9092, 9093)
      .withNetworkAliases("broker")
      .withEnv("KAFKA_HOST_NAME", "broker")
      .withEnv("KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR", "1")
      .withEnv("KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR", "1")
      .also { container -> container.start() }
  )
