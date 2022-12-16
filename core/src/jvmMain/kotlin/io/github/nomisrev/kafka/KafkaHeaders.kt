package io.github.nomisrev.kafka

import org.apache.kafka.common.header.Header as KafkaHeader
import org.apache.kafka.common.header.Headers as KafkaHeaders
import org.apache.kafka.common.header.internals.RecordHeaders

actual interface Header : KafkaHeader {
  actual val key: String?
  actual val value: ByteArray?

  fun toKafkaHeader(): org.apache.kafka.common.header.Header = object : KafkaHeader {
    override fun key(): String? = key
    override fun value(): ByteArray? = value
  }
}

@JvmInline
private value class HeaderWrapper(val header: KafkaHeader) : Header, KafkaHeader by header {
  override val key: String?
    get() = header.key()
  override val value: ByteArray?
    get() = header.value()
}

fun KafkaHeader.toHeader(): Header =
  HeaderWrapper(this)

fun KafkaHeaders.toHeaders(): Headers =
  mutableListOf<Header>().apply {
    this@toHeaders.forEach { add(it.toHeader()) }
  }

fun Headers.toKafkaHeaders(): KafkaHeaders =
  RecordHeaders(map { it.toKafkaHeader() })
