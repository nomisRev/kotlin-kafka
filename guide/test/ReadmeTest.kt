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
            "test-topic-0@10",
            "test-topic-0@11",
            "test-topic-0@12",
            "test-topic-0@13",
            "test-topic-0@14",
            "test-topic-0@15",
            "test-topic-0@16",
            "test-topic-0@17",
            "test-topic-0@18",
            "test-topic-0@19",
            "Key(index=1) -> Message(content=msg: 1)",
            "Key(index=2) -> Message(content=msg: 2)",
            "Key(index=3) -> Message(content=msg: 3)",
            "Key(index=4) -> Message(content=msg: 4)",
            "Key(index=5) -> Message(content=msg: 5)",
            "Key(index=6) -> Message(content=msg: 6)",
            "Key(index=7) -> Message(content=msg: 7)",
            "Key(index=8) -> Message(content=msg: 8)",
            "Key(index=9) -> Message(content=msg: 9)",
            "Key(index=10) -> Message(content=msg: 10)",
            "Key(index=11) -> Message(content=msg: 11)",
            "Key(index=12) -> Message(content=msg: 12)",
            "Key(index=13) -> Message(content=msg: 13)",
            "Key(index=14) -> Message(content=msg: 14)",
            "Key(index=15) -> Message(content=msg: 15)",
            "Key(index=16) -> Message(content=msg: 16)",
            "Key(index=17) -> Message(content=msg: 17)",
            "Key(index=18) -> Message(content=msg: 18)",
            "Key(index=19) -> Message(content=msg: 19)",
            "Key(index=20) -> Message(content=msg: 20)"
        )
    }
}
