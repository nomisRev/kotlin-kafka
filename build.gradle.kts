import com.bnorm.power.PowerAssertGradleExtension
import kotlinx.knit.KnitPluginExtension
import org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
import org.gradle.api.tasks.testing.logging.TestLogEvent.*
import org.jetbrains.dokka.gradle.DokkaTask

plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.dokka)
  alias(libs.plugins.spotless)
  alias(libs.plugins.knit)
  alias(libs.plugins.publish)
  alias(libs.plugins.power.assert)
  idea
}

repositories {
  mavenCentral()
  maven("https://oss.sonatype.org/content/repositories/snapshots")
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
  testImplementation(libs.kotlinx.coroutines.test)
  testImplementation(libs.jackson.kotlin)
  testImplementation(libs.jackson.databind)
  testImplementation(libs.kotest.framework.api)
  testImplementation(libs.kotest.runner.junit5)
  testImplementation(libs.kotest.property)
  testImplementation(libs.stove.testing)
  testImplementation(libs.stove.testing.kafka)
  testImplementation(libs.logback.classic)
}

configure<PowerAssertGradleExtension> {
  functions = listOf("kotlin.test.assertEquals")
}

configure<KnitPluginExtension> {
  siteRoot = "https://nomisrev.github.io/kotlin-kafka/"
}

configure<JavaPluginExtension> {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(8))
  }
}

sourceSets {
  @Suppress("LocalVariableName", "ktlint:standard:property-naming")
  val `test-e2e` by creating {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
  }

  val testE2eImplementation by configurations.getting {
    extendsFrom(configurations.testImplementation.get())
  }
  configurations["testE2eRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())
}

idea {
  module {
    testSources.from(sourceSets["test-e2e"].allSource.sourceDirectories)
    testResources.from(sourceSets["test-e2e"].resources.sourceDirectories)
    isDownloadJavadoc = true
    isDownloadSources = true
  }
}

kotlin {
  explicitApi()
  jvmToolchain(17)
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
    jvmArgs("--add-opens", "java.base/java.util=ALL-UNNAMED")
  }

  task<Test>("e2eTest") {
    description = "Runs e2e tests."
    group = "verification"
    testClassesDirs = sourceSets["test-e2e"].output.classesDirs
    classpath = sourceSets["test-e2e"].runtimeClasspath

    useJUnitPlatform()
    reports {
      junitXml.required.set(true)
      html.required.set(true)
    }
    jvmArgs("--add-opens", "java.base/java.util=ALL-UNNAMED")
  }
}
