package io.github.nomisrev.kafka

import io.kotest.core.config.AbstractProjectConfig
import io.kotest.property.PropertyTesting
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

object KotestProject : AbstractProjectConfig() {
  init {
    System.setProperty("kotest.assertions.collection.print.size", "100")
    PropertyTesting.defaultIterationCount = 10
  }

  override val timeout: Duration = 20.seconds
  override val invocationTimeout: Long = 20_000L
}