import kotlinx.knit.KnitPluginExtension
import org.gradle.api.tasks.testing.logging.TestExceptionFormat.*
import org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED
import org.gradle.api.tasks.testing.logging.TestLogEvent.SKIPPED
import org.gradle.api.tasks.testing.logging.TestLogEvent.STANDARD_ERROR
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_2_0

plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.kotlin.assert)
  alias(libs.plugins.dokka)
  alias(libs.plugins.spotless)
  alias(libs.plugins.knit)
  alias(libs.plugins.publish)
}

repositories {
  mavenCentral()
}

group = "io.github.nomisrev"

dependencies {
  api(libs.kotlin.stdlib)
  api(libs.kotlinx.coroutines.core)
  api(libs.kotlinx.coroutines.jdk8)
  api(libs.kafka.clients)
  implementation(libs.slf4j.api)

  testImplementation(kotlin("test"))
  testImplementation(libs.testcontainers.kafka)
  testImplementation(libs.slf4j.simple)
  testImplementation(libs.kotlinx.coroutines.test)
}

@Suppress("OPT_IN_USAGE")
powerAssert {
  functions = listOf("kotlin.test.assertEquals")
}

//configure<PowerAssertGradleExtension> {
//  functions = listOf("kotlin.test.assertEquals")
//}

configure<KnitPluginExtension> {
  siteRoot = "https://nomisrev.github.io/kotlin-kafka/"
}

configure<JavaPluginExtension> {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(8))
  }
}

kotlin {
  explicitApi()
  compilerOptions {
    languageVersion.set(KOTLIN_2_0)
    apiVersion.set(KOTLIN_2_0)
  }
}

tasks {
  withType<DokkaTask>().configureEach {
    outputDirectory.set(rootDir.resolve("docs"))
    moduleName.set("kotlin-kafka")
    dokkaSourceSets {
      named("main") {
        includes.from("README.md")
        perPackageOption {
          matchingRegex.set(".*\\.internal.*")
          suppress.set(true)
        }
        sourceLink {
          localDirectory.set(file("src/main/kotlin"))
          remoteUrl.set(uri("https://github.com/nomisRev/kotlin-kafka/tree/main/src/main/kotlin").toURL())
          remoteLineSuffix.set("#L")
        }
      }
    }
  }

  getByName("knitPrepare").dependsOn(getTasksByName("dokka", true))

  withType<Test>().configureEach {
    useJUnitPlatform()
    maxParallelForks = (2 * Runtime.getRuntime().availableProcessors())
    if (project.hasProperty("stressTest")) {
      systemProperty("io.github.nomisrev.kafka.TEST_ITERATIONS", project.properties["stressTest"] ?: 100)
    }
    testLogging {
      exceptionFormat = FULL
      events = setOf(SKIPPED, FAILED, STANDARD_ERROR)
    }
  }
}
