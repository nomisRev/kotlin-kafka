package io.github.nomisrev.kafka.consumer

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.map
import io.kotest.property.checkAll
import java.lang.IllegalArgumentException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

class CommitStrategySpec : StringSpec({
  "Negative or zero sized BySize strategy fails" {
    checkAll(Arb.int(max = 0)) { size ->
      shouldThrow<IllegalArgumentException> {
        CommitStrategy.BySize(size)
      }.message shouldBe "Size based auto-commit requires positive non-zero commit batch size but found $size"
    }
  }
  
  "Negative or zero sized BySizeOrTime strategy fails" {
    checkAll(Arb.int(max = 0)) { size ->
      shouldThrow<IllegalArgumentException> {
        CommitStrategy.BySizeOrTime(size, 1.seconds)
      }.message shouldBe "Size based auto-commit requires positive non-zero commit batch size but found $size"
    }
  }
  
  fun Arb.Companion.duration(
    min: Long = Long.MIN_VALUE, max: Long = Long.MAX_VALUE,
  ): Arb<Duration> = Arb.long(min, max).map { it.nanoseconds }
  
  "Negative or zero duration BySizeOrTime strategy fails" {
    checkAll(Arb.duration(max = 0)) { duration ->
      shouldThrow<IllegalArgumentException> {
        CommitStrategy.BySizeOrTime(1, duration)
      }.message shouldBe "Time based auto-commit requires positive non-zero interval but found $duration"
    }
  }
  
  "Negative or zero duration ByTime strategy fails" {
    checkAll(Arb.duration(max = 0)) { duration ->
      shouldThrow<IllegalArgumentException> {
        CommitStrategy.ByTime(duration)
      }.message shouldBe "Time based auto-commit requires positive non-zero interval but found $duration"
    }
  }
})
