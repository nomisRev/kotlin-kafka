package io.github.nomisrev.kafka

import io.kotest.core.config.AbstractProjectConfig
import io.kotest.property.PropertyTesting

object KotestProject : AbstractProjectConfig() {
  init {
    System.setProperty("kotest.assertions.collection.print.size", "100")
    PropertyTesting.defaultIterationCount = 10
  }
}