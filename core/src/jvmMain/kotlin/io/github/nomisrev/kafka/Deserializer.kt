package io.github.nomisrev.kafka

import org.apache.kafka.common.header.Headers

fun <T> Deserializer<T>.toJavaDeserializer(): org.apache.kafka.common.serialization.Deserializer<T> =
  object : org.apache.kafka.common.serialization.Deserializer<T> {
    override fun configure(configs: Map<String, *>?, isKey: Boolean) =
      this@toJavaDeserializer.configure(configs, isKey)

    override fun deserialize(topic: String?, data: ByteArray?): T? =
      this@toJavaDeserializer.deserialize(topic, data)

    override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): T? =
      this@toJavaDeserializer.deserialize(topic, data, headers?.toHeaders())
  }