package io.github.nomisRev.kafka.e2e.setup

import com.trendyol.stove.testing.e2e.standalone.kafka.KafkaSystemOptions
import com.trendyol.stove.testing.e2e.standalone.kafka.kafka
import com.trendyol.stove.testing.e2e.system.TestSystem
import io.kotest.core.config.AbstractProjectConfig

class ProjectConfig : AbstractProjectConfig() {
  override suspend fun beforeProject(): Unit = TestSystem()
    .with {
      kafka {
        KafkaSystemOptions(
          configureExposedConfiguration = { cfg ->
            listOf(
              "kafka.servers=${cfg.bootstrapServers}",
              "kafka.interceptor-classes=${cfg.interceptorClass}",
              "kafka.receive-method=kotlin-kafka" // here we can change to: 'kotlin-kafka' or 'traditional'
            )
          }
        )
      }
      applicationUnderTest(KafkaApplicationUnderTest())
    }.run()

  override suspend fun afterProject(): Unit = TestSystem.stop()
}
