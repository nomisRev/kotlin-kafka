package io.github.nomisrev.kafka.receiver

import io.github.nomisRev.kafka.receiver.CommitStrategy
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.lang.IllegalArgumentException
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class CommitStrategySpec {
  @Test
  fun `Negative or zero sized BySize strategy fails`() = runBlocking<Unit> {
    val actual = assertThrows<IllegalArgumentException> {
      CommitStrategy.BySize(0)
    }.message
    assertEquals(
      "Size based auto-commit requires positive non-zero commit batch size but found 0",
      actual
    )
  }

  @Test
  fun `Negative or zero sized BySizeOrTime strategy fails`() = runBlocking<Unit> {
    val actual = assertThrows<IllegalArgumentException> {
      CommitStrategy.BySizeOrTime(0, 1.seconds)
    }.message
    assertEquals(
      "Size based auto-commit requires positive non-zero commit batch size but found 0",
      actual
    )
  }

  @Test
  fun `Negative or zero duration BySizeOrTime strategy fails`() = runBlocking<Unit> {
    val actual = assertThrows<IllegalArgumentException> {
      CommitStrategy.BySizeOrTime(1, 0.seconds)
    }.message
    assertEquals(
      "Time based auto-commit requires positive non-zero interval but found ${0.seconds}",
      actual
    )
  }

  @Test
  fun `Negative or zero duration ByTime strategy fails`() = runBlocking<Unit> {
    val actual = assertThrows<IllegalArgumentException> {
      CommitStrategy.ByTime(0.seconds)
    }.message
    assertEquals(
      "Time based auto-commit requires positive non-zero interval but found ${0.seconds}",
      actual
    )
  }
}
