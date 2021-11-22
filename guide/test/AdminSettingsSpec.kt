// This file was automatically generated from Admin.kt by Knit tool. Do not edit.
package example.test

import org.junit.Test
import kotlinx.knit.test.*

class AdminSettingsSpec {
    @Test
    fun testExampleAdmin01() {
        captureOutput("ExampleAdmin01") { example.exampleAdmin01.main() }.also { lines ->
            check(lines.isNotEmpty())
        }
    }
}
