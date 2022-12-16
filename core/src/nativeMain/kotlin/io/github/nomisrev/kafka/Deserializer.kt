package io.github.nomisrev.kafka

import com.icemachined.kafka.clients.consumer.Headers

fun <T> Deserializer<T>.toNativeDeserializer(): com.icemachined.kafka.common.serialization.Deserializer<T> =
  object : com.icemachined.kafka.common.serialization.Deserializer<T> {
    override fun deserialize(data: ByteArray, topic: String?, headers: Headers?): T? =
      this@toNativeDeserializer.deserialize(topic, data, headers)

    override fun configure(configs: Map<String?, *>?, isKey: Boolean) =
      this@toNativeDeserializer.configure(
        configs?.mapKeys { it.key.toString() },
        isKey
      )
  }