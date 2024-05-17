package io.github.nomisRev.kafka.e2e.setup.example

/**
 * Receive method to change the way of receiving messages from Kafka
 */
enum class ReceiveMethod {
  /**
   * Using Kotlin Kafka receiver
   */
  KOTLIN_KAFKA_RECEIVE,

  /**
   * Using traditional while(true) loop to receive messages
   */
  TRADITIONAL_RECEIVE;

  companion object {
    fun from(value: String): ReceiveMethod = value.toKafkaReceiverMethod()
    private fun String.toKafkaReceiverMethod(): ReceiveMethod = when (this) {
      "kotlin-kafka" -> KOTLIN_KAFKA_RECEIVE
      "traditional" -> TRADITIONAL_RECEIVE
      else -> error("Unknown receive method: $this")
    }
  }

}
