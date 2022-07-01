package io.github.nomisRev.kafka

import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

/**
 * A [Serializer] for [Nothing], this way we signal in a typed way that `Key` is not used for a
 * certain topic.
 */
public object NothingSerializer : Serializer<Nothing> {
  override fun close(): Unit = Unit
  override fun configure(configs: MutableMap<String, *>?, isKey: Boolean): Unit = Unit
  override fun serialize(topic: String?, data: Nothing?): ByteArray = ByteArray(0)
}

/**
 * A [Deserializer] for [Nothing], this way we signal in a typed way that `Key` is not used for a
 * certain topic.
 */
public object NothingDeserializer : Deserializer<Nothing> {
  override fun close(): Unit = Unit
  override fun configure(configs: MutableMap<String, *>?, isKey: Boolean): Unit = Unit
  override fun deserialize(topic: String?, data: ByteArray?): Nothing = TODO("Impossible")
}

public fun <A, B> Serializer<A>.imap(f: (B) -> A): Serializer<B> = MappedSerializer(this, f)

private class MappedSerializer<A, B>(val original: Serializer<A>, val imap: (B) -> A) :
  Serializer<B> {
  override fun close() = original.close()

  override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) =
    original.configure(configs, isKey)

  override fun serialize(topic: String?, headers: Headers?, data: B): ByteArray =
    original.serialize(topic, headers, imap(data))

  override fun serialize(topic: String?, data: B): ByteArray = original.serialize(topic, imap(data))
}

public fun <A, B> Deserializer<A>.map(f: (A) -> B): Deserializer<B> = MappedDeserializer(this, f)

private class MappedDeserializer<A, B>(val original: Deserializer<A>, val map: (A) -> B) :
  Deserializer<B> {
  override fun close() = original.close()

  override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) =
    original.configure(configs, isKey)

  override fun deserialize(topic: String?, data: ByteArray?): B =
    original.deserialize(topic, data).let(map)

  override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): B =
    original.deserialize(topic, headers, data).let(map)
}
