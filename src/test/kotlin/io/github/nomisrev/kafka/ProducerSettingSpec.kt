package io.github.nomisrev.kafka

import io.github.nomisRev.kafka.Acks
import io.github.nomisRev.kafka.ProducerSettings
import io.kotest.assertions.assertSoftly
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.maps.shouldContainAll
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.enum
import io.kotest.property.arbitrary.map
import io.kotest.property.arbitrary.string
import io.kotest.property.checkAll
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class ProducerSettingSpec : StringSpec({
  
  "ProducerSettings Ack" {
    checkAll(
      Arb.string(),
      Arb.enum<Acks>(),
      Arb.map(Arb.string(), Arb.string())
    ) { bootstrapServers, acks, map ->
      val settings = ProducerSettings<String, String>(
        bootstrapServers,
        StringSerializer(),
        StringSerializer(),
        acks = acks,
        other = Properties().apply {
          putAll(map)
        }
      )
      
      assertSoftly(settings.properties()) {
        @Suppress("UNCHECKED_CAST")
        toMap().shouldContainAll(map as Map<Any, Any>)
        getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) shouldBe bootstrapServers
        getProperty(ProducerConfig.ACKS_CONFIG) shouldBe acks.value
      }
    }
  }
})