// This file was automatically generated from Admin.kt by Knit tool. Do not edit.
package example.test

import org.junit.jupiter.api.Test
import kotlinx.knit.test.*
import com.ginsberg.junit.exit.ExpectSystemExitWithStatus

class AdminSpec {
    @Test
    @ExpectSystemExitWithStatus(0)
    fun testExampleAdmin01() {
        captureOutput("ExampleAdmin01") { example.exampleAdmin01.main() }.also { lines ->
            check(lines.isNotEmpty())
        }
    }
}
