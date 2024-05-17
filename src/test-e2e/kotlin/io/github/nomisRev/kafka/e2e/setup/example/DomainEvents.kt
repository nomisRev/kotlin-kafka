package io.github.nomisRev.kafka.e2e.setup.example

object DomainEvents {
  data class ProductCreated(val productId: String)
}
