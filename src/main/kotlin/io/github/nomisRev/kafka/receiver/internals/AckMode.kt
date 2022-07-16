package io.github.nomisRev.kafka.receiver.internals

internal enum class AckMode {
  AUTO_ACK, MANUAL_ACK, ATMOST_ONCE, EXACTLY_ONCE
}
