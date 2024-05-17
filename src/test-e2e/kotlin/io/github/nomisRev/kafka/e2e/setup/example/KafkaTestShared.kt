package io.github.nomisRev.kafka.e2e.setup.example

import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.receiver.KafkaReceiver

object KafkaTestShared {
  data class TopicDefinition(
    val topic: String,
    val retryTopic: String,
    val deadLetterTopic: String
  )

  val topics = listOf(
    TopicDefinition("product", "product.retry", "product.error"),
  )
  val consumers: (
    kafkaReceiver: KafkaReceiver<String, Any>,
    producerSettings: KafkaPublisher<String, Any>,
    receiveMethod: ReceiveMethod
  ) -> List<ConsumerSupervisor<String, Any>> = { a, b, r -> listOf(ProductConsumer(a, b, r)) }
}
