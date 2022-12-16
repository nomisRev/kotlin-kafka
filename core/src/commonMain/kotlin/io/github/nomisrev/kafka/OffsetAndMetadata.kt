package io.github.nomisrev.kafka

data class OffsetAndMetadata(
  val offset: Long,
  val leaderEpoch: Int? = null,
  val metadata: String? = ""
)
