package io.github.nomisRev.kafka

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.io.Closeable

@Deprecated(
  "Will be removed in Kotlin-Kafka 0.4.x",
  ReplaceWith(
    "flow { use { emit(it) } }",
    "kotlinx.coroutines.flow.flow"
  )
)
public fun <A : Closeable> A.asFlow(): Flow<A> =
  flow { use { emit(it) } }
