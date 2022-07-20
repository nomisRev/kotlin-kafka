package io.github.nomisrev.kafka

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

inline fun <A, B> Flow<A>.mapIndexed(
  crossinline transform: suspend (index: Int, value: A) -> B,
): Flow<B> {
  var index = 0
  return map { value ->
    transform(index++, value)
  }
}