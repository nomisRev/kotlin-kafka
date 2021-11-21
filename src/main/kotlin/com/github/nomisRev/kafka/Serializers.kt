package com.github.nomisRev.kafka

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

/**
 * A [Serializer] for [Nothing], this way we signal in a typed way that `Key` is not used for a
 * certain topic.
 */
object NothingSerializer : Serializer<Nothing> {
  override fun close() = Unit
  override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) = Unit
  override fun serialize(topic: String?, data: Nothing?): ByteArray = ByteArray(0)
}

/**
 * A [Deserializer] for [Nothing], this way we signal in a typed way that `Key` is not used for a
 * certain topic.
 */
object NothingDeserializer : Deserializer<Nothing> {
  override fun close() = Unit
  override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) = Unit
  override fun deserialize(topic: String?, data: ByteArray?): Nothing = TODO("Impossible")
}
