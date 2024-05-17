package io.github.nomisRev.kafka.e2e.setup.example

import io.github.nomisRev.kafka.e2e.setup.example.KafkaTestShared.TopicDefinition
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Just a regular consumer, nothing special, listening to the product, product.retry and product.error topics
 */
class ProductConsumer(
  kafkaReceiver: KafkaReceiver<String, Any>,
  kafkaPublisher: KafkaPublisher<String, Any>,
  receiveMethod: ReceiveMethod
) : ConsumerSupervisor<String, Any>(kafkaReceiver, kafkaPublisher, receiveMethod) {
  private val logger = org.slf4j.LoggerFactory.getLogger(javaClass)
  override val topicDefinition: TopicDefinition = TopicDefinition("product", "product.retry", "product.error")
  override suspend fun consume(record: ConsumerRecord<String, Any>) {
    logger.info("ProductConsumer received: ${record.value()}")
  }
}
