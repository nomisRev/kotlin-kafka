package io.github.nomisRev.kafka.e2e.tests

import arrow.core.some
import com.trendyol.stove.testing.e2e.standalone.kafka.kafka
import com.trendyol.stove.testing.e2e.system.TestSystem.Companion.validate
import io.github.nomisRev.kafka.e2e.setup.example.DomainEvents.ProductCreated
import io.kotest.core.spec.style.FunSpec
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

class KafkaSystemTests : FunSpec({
  val randomString = { Random.nextInt(0, Int.MAX_VALUE).toString() }

  test("message should be committed and consumed successfully") {
    validate {
      kafka {
        val productId = randomString() + "[productCreated]"
        publish("product", message = ProductCreated(productId), key = randomString().some())
        shouldBeConsumed<ProductCreated>(10.seconds) {
          actual.productId == productId
        }
      }
    }
  }
})
