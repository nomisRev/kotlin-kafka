package io.github.nomisrev.kafka

import io.github.nomisRev.kafka.internal.chunked
import io.kotest.assertions.assertSoftly
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.map
import io.kotest.property.checkAll
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlin.math.absoluteValue
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@OptIn(ExperimentalTime::class)
@ExperimentalCoroutinesApi
class ChunkSpec : StringSpec({
  
  "should never lose any elements, and get full chunks if not timing out" {
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
  
  val infinite = flow {
    while (true) {
      currentCoroutineContext().ensureActive()
      emit(Unit)
    }
  }
  
  "Can take from infinite stream" {
    runTest {
      checkAll(
        Arb.int(min = 1, max = 50), // chunk needs to have minSize 1
        Arb.int(min = 1, max = 50), // chunk needs to have minSize 1
        Arb.long(min = 100).map { it.milliseconds } // During warm-up can take up to 100ms for first flow
      ) { size, count, timeout ->
        infinite.map {
          delay(100.microseconds)
        }.chunked(size, timeout)
          .take(count)
          .toList() shouldBe List(count) { List(size) { } }
      }
    }
  }
  
  "empty flow" {
    runTest {
      val empty = emptyFlow<Int>()
      checkAll(
        Arb.int(min = 1), // chunk needs to have minSize 1
        Arb.long(min = 15).map { it.milliseconds } // During warm-up can take up to 10ms for first flow
      ) { size, timeout ->
        val (value, duration) = measureTimedValue {
          empty.chunked(size, timeout).toList()
        }
        value.shouldBeEmpty()
        duration.inWholeMilliseconds < 15
      }
    }
  }
  
  "multiple elements not filling chunks" {
    runTest {
      val flow = flow {
        for (i in 1..5) {
          delay(500)
          emit(i)
        }
      }
      
      val result = flow.chunked(500, 1100.milliseconds).toList()
      assertSoftly(result) {
        size shouldBe 3
        first().size shouldBe 2
        get(1).size shouldBe 2
        get(2).size shouldBe 1
      }
    }
  }
})
