package io.github.nomisrev.kafka.receiver

internal enum class AckMode {
  AUTO_ACK, MANUAL_ACK, ATMOST_ONCE, EXACTLY_ONCE
}
