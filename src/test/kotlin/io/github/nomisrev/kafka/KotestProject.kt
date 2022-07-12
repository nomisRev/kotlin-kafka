package io.github.nomisrev.kafka

import io.kotest.core.config.AbstractProjectConfig
import io.kotest.property.PropertyTesting

object KotestProject : AbstractProjectConfig() {
  init {
    PropertyTesting.defaultIterationCount = 10
  }
}