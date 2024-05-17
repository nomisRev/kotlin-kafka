package io.github.nomisRev.kafka.e2e.setup.example

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer


private val stoveKafkaObjectMapperRef = ObjectMapper()

@Suppress("UNCHECKED_CAST")
class StoveKafkaValueDeserializer<T : Any> : Deserializer<T> {
  override fun deserialize(
    topic: String,
    data: ByteArray
  ): T = stoveKafkaObjectMapperRef.readValue<Any>(data) as T
}

class StoveKafkaValueSerializer<T : Any> : Serializer<T> {
  override fun serialize(
    topic: String,
    data: T
  ): ByteArray = stoveKafkaObjectMapperRef.writeValueAsBytes(data)
}
