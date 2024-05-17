package io.github.nomisRev.kafka.e2e.setup.example

import io.github.nomisRev.kafka.e2e.setup.example.KafkaTestShared.TopicDefinition
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.flattenConcat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration

/**
 * Supervisor that uses KafkaReceiver to retrieve messages from Kafka and handle them accordingly
 */
abstract class ConsumerSupervisor<K, V>(
  private val receiver: KafkaReceiver<K, V>,
  private val publisher: KafkaPublisher<K, V>,
  private val receiveMethod: ReceiveMethod
) : AutoCloseable {
  private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
  private val logger = org.slf4j.LoggerFactory.getLogger(javaClass)

  abstract val topicDefinition: TopicDefinition

  /**
   * Here we start the consumer
   * We can use either KotlinKafka receiver or traditional while(true) loop to receive messages
   * Traditional while(true) loop is successful in receiving messages continuously
   * KotlinKafka receiver, continuously receives messages
   */
  fun start() = when (receiveMethod) {
    ReceiveMethod.KOTLIN_KAFKA_RECEIVE -> kotlinKafkaReceive()
    ReceiveMethod.TRADITIONAL_RECEIVE -> traditionalReceive()
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  private fun kotlinKafkaReceive() {
    scope.launch {
      receiver.receiveAutoAck(
        listOf(
          topicDefinition.topic,
          topicDefinition.retryTopic,
          topicDefinition.deadLetterTopic
        )
      ).flattenConcat()
        .collect { message ->
          logger.info("Message RECEIVED on the application side with KotlinKafka receiver: ${message.value()}")
          received(message) // expected to receive the messages continuously?
        }
    }
  }

  private fun traditionalReceive() {
    scope.launch {
      receiver.withConsumer { consumer ->
        consumer.subscribe(
          listOf(
            topicDefinition.topic,
            topicDefinition.retryTopic,
            topicDefinition.deadLetterTopic
          )
        )
        while (isActive) {
          val records = consumer.poll(Duration.ofMillis(500))
          records.forEach { record ->
            logger.info("Message RECEIVED on the application side with traditional while(true) loop: ${record.value()}")
            received(record) {
              consumer.commitAsync()
            }
          }
        }
      }
    }
  }


  abstract suspend fun consume(record: ConsumerRecord<K, V>)

  protected open suspend fun handleError(message: ConsumerRecord<K, V>, e: Exception) {
    logger.error("Failed to process message: $message", e)
  }

  private suspend fun received(message: ConsumerRecord<K, V>, onSuccess: (ConsumerRecord<K, V>) -> Unit = { }) {
    try {
      consume(message)
      onSuccess(message)
      logger.info("Message COMMITTED on the application side: ${message.value()}")
    } catch (e: Exception) {
      handleError(message, e)
      logger.warn("CONSUMER GOT an ERROR on the application side, exception: $e")
      val record = ProducerRecord<K, V>(
        topicDefinition.deadLetterTopic,
        message.partition(),
        message.key(),
        message.value(),
        message.headers()
      )
      try {
        publisher.publishScope { offer(record) }
      } catch (e: Exception) {
        logger.error("Failed to publish message to dead letter topic: $message", e)
      }
    }
  }

  override fun close(): Unit = runBlocking {
    try {
      scope.cancel()
    } catch (e: Exception) {
      logger.error("Failed to stop consuming", e)
    }
  }
}
