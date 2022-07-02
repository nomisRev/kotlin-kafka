package io.github.nomisRev.kafka

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.io.Closeable

public fun <A : Closeable> A.asFlow(): Flow<A> =
  flow { use { emit(it) } }
