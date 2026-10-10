package io.github.nomisrev.kafka

import io.github.nomisRev.kafka.AdminSettings
import io.github.nomisRev.kafka.NothingDeserializer
import io.github.nomisRev.kafka.NothingSerializer
import io.github.nomisRev.kafka.publisher.Acks
import io.github.nomisRev.kafka.publisher.PublisherSettings
import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertSame

class SettingsFromPropertiesSpec {
  private fun props(vararg pairs: Pair<String, Any>): Properties =
    Properties().apply { putAll(pairs.toMap()) }

  @Test
  fun `AdminSettings reads bootstrap servers`() {
    val properties = props(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092", "client.id" to "admin")
    val settings = AdminSettings(properties)
    assertEquals("localhost:9092", settings.bootstrapServer)
    assertEquals("admin", settings.properties()["client.id"])
  }

  @Test
  fun `AdminSettings joins list bootstrap servers`() {
    val properties = props(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to listOf("a:9092", "b:9092"))
    assertEquals("a:9092,b:9092", AdminSettings(properties).bootstrapServer)
  }

  @Test
  fun `AdminSettings fails without bootstrap servers`() {
    val message = assertThrows<IllegalArgumentException> { AdminSettings(Properties()) }.message
    assertEquals("Missing required property 'bootstrap.servers'", message)
  }

  @Test
  fun `ReceiverSettings reads required and optional properties`() {
    val properties = props(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG to "group",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "latest",
    )
    val settings = ReceiverSettings(properties, StringDeserializer(), StringDeserializer())
    assertEquals("localhost:9092", settings.bootstrapServers)
    assertEquals("group", settings.groupId)
    assertEquals(AutoOffsetReset.Latest, settings.autoOffsetReset)
    assertSame(properties, settings.properties)
  }

  @Test
  fun `ReceiverSettings without key uses NothingDeserializer and defaults auto offset reset`() {
    val properties = props(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG to "group",
    )
    val settings = ReceiverSettings(properties, StringDeserializer())
    assertSame(NothingDeserializer, settings.keyDeserializer)
    assertEquals(AutoOffsetReset.Earliest, settings.autoOffsetReset)
  }

  @Test
  fun `ReceiverSettings fails without group id`() {
    val properties = props(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092")
    val message = assertThrows<IllegalArgumentException> {
      ReceiverSettings(properties, StringDeserializer())
    }.message
    assertEquals("Missing required property 'group.id'", message)
  }

  @Test
  fun `ReceiverSettings fails with invalid auto offset reset`() {
    val properties = props(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG to "group",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "wrong",
    )
    val message = assertThrows<IllegalArgumentException> {
      ReceiverSettings(properties, StringDeserializer())
    }.message
    assertEquals(
      "Invalid value 'wrong' for property 'auto.offset.reset', expected one of earliest, latest, none",
      message
    )
  }

  @Test
  fun `PublisherSettings reads required and optional properties`() {
    val properties = props(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
      ProducerConfig.ACKS_CONFIG to "1",
    )
    val settings = PublisherSettings(properties, StringSerializer(), StringSerializer())
    assertEquals("localhost:9092", settings.bootstrapServers)
    assertEquals(Acks.One, settings.acknowledgments)
    assertSame(properties, settings.properties)
  }

  @Test
  fun `PublisherSettings without key maps -1 acks to All`() {
    val properties = props(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
      ProducerConfig.ACKS_CONFIG to "-1",
    )
    val settings = PublisherSettings(properties, StringSerializer())
    assertSame(NothingSerializer, settings.keySerializer)
    assertEquals(Acks.All, settings.acknowledgments)
  }

  @Test
  fun `PublisherSettings fails without bootstrap servers`() {
    val message = assertThrows<IllegalArgumentException> {
      PublisherSettings(Properties(), StringSerializer())
    }.message
    assertEquals("Missing required property 'bootstrap.servers'", message)
  }
}
