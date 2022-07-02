package io.github.nomisrev.kafka

import io.github.nomisRev.kafka.internal.chunked
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.checkAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlin.math.absoluteValue
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.microseconds

class ChunkSpec : StringSpec({
  
  "should never lose any elements" {
    runTest {
      checkAll(
        Arb.list(Arb.int()),
        Arb.int(min = 1) // chunk needs to have minSize 1
      ) { source, maxGroupSize0 ->
        val maxGroupSize = (maxGroupSize0 % 20).absoluteValue + 1
        
        source.asFlow().map { i ->
          delay(100.microseconds)
          i
        }.chunked(maxGroupSize, 2.days).toList() shouldBe source.chunked(maxGroupSize)
      }
    }
  }
  
  // @Test
  // fun testEmptyFlowTimeOrSizeBasedChunking() = runTest {
  //   val emptyFlow = emptyFlow<Int>()
  //   val result = measureTimedValue {
  //     emptyFlow.chunked(ChunkingMethod.ByTimeOrSize(intervalMs = 10 * 1000, maxSize = 5)).toList()
  //   }
  //   assertTrue(result.value.isEmpty())
  //   assertTrue(result.duration < 500.milliseconds)
  // }
  //
  // @Test
  // fun testMultipleElementsFillingBufferWithTimeOrSizeBasedChunking() = runTest {
  //   val flow = flow<Int> {
  //     for (i in 1..10) {
  //       emit(i)
  //     }
  //   }
  //   val result = measureTimedValue {
  //     flow.chunked(ChunkingMethod.ByTimeOrSize(intervalMs = 10 * 1000, maxSize = 5)).toList()
  //   }
  //   assertEquals(2, result.value.size)
  //   assertEquals(5, result.value.first().size)
  //   assertEquals(5, result.value[1].size)
  //   assertTrue(result.duration < 500.milliseconds)
  // }
  //
  // @Test
  // fun testMultipleElementsNotFillingBufferWithTimeOrSizeBasedChunking() = withVirtualTime {
  //   val flow = flow {
  //     for (i in 1..5) {
  //       delay(500)
  //       emit(i)
  //     }
  //   }
  //   val result = flow.chunked(ChunkingMethod.ByTimeOrSize(intervalMs = 1100, maxSize = 500)).toList()
  //
  //   assertEquals(3, result.size)
  //   assertEquals(2, result.first().size)
  //   assertEquals(2, result[1].size)
  //   assertEquals(1, result[2].size)
  //
  //   finish(1)
  // }
})