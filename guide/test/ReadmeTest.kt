// This file was automatically generated from README.md by Knit tool. Do not edit.
package example.test

import org.junit.Test
import kotlinx.knit.test.*

class ReadmeTest {
    @Test
    fun testExampleReadme01() {
        captureOutput("ExampleReadme01") { example.exampleReadme01.main() }.verifyOutputLines(
            "test-topic-0@0",
            "test-topic-0@1",
            "test-topic-0@2",
            "test-topic-0@3",
            "test-topic-0@4",
            "test-topic-0@5",
            "test-topic-0@6",
            "test-topic-0@7",
            "test-topic-0@8",
            "test-topic-0@9",
            "Key(index=1) -> Message(content=msg: 1)",
            "Key(index=2) -> Message(content=msg: 2)",
            "Key(index=3) -> Message(content=msg: 3)",
            "Key(index=4) -> Message(content=msg: 4)",
            "Key(index=5) -> Message(content=msg: 5)",
            "Key(index=6) -> Message(content=msg: 6)",
            "Key(index=7) -> Message(content=msg: 7)",
            "Key(index=8) -> Message(content=msg: 8)",
            "Key(index=9) -> Message(content=msg: 9)",
            "Key(index=10) -> Message(content=msg: 10)"
        )
    }
}
