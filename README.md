# KafkaPlayground

Small playground where I'm playing around with Kafka in Kotlin and the Kafka SDK whilst reading the Kafka book Definite Guide from Confluent.
https://www.confluent.io/resources/kafka-the-definitive-guide-v2/

Goals:
 1. Play around with Kafka in Kotlin.
 2. Setup an example for testing Kafka with Test Containers in Kotlin.
 3. Write a Kotlin Kafka extension. Potential library to conveniently work with Kafka in Kotlin.
     - It should come with cancellation support for suspend (KotlinX Coroutines).
     - It should be easy to write, and compose KotlinX Flow based programs for streaming Kafka with the Java SDK.

### Repo
#### Code
There is an `example`, and `kafkaContainer` file in the root that has the content of goal `1` & `2`.
And in the `com.github.nomisrev.kafka` package you can find the Kafka Kotlin extensions.
