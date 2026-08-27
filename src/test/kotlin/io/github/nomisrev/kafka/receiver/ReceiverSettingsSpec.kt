package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.receiver.ReceiverSettings
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test

class ReceiverSettingsSpec {

  @Test
  fun `the keyless ReceiverSettings hands its createConsumer on`() = runBlocking {
    val consumer = MockConsumer<Nothing, String>(OffsetResetStrategy.EARLIEST)
    val properties = Properties().apply { put("client.id", "keyless-forwarding") }

    val settings = ReceiverSettings(
      bootstrapServers = "unused:9092",
      valueDeserializer = StringDeserializer(),
      groupId = "keyless-group",
      properties = properties,
      createConsumer = { consumer },
    )

    assertSame(consumer, settings.createConsumer(settings))
    /* The two parameters next to createConsumer, so that this also fails if the overload ever
     * stops handing its arguments on by name. */
    assertEquals(properties, settings.properties)
    assertEquals("keyless-group", settings.groupId)
  }
}
