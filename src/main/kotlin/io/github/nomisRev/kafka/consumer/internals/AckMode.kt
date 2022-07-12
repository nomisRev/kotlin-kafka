package io.github.nomisRev.kafka.consumer.internals

internal enum class AckMode {
  // AUTO_ACK, MANUAL_ACK, ATMOST_ONCE, EXACTLY_ONCE
  AUTO_ACK, MANUAL_ACK
}
