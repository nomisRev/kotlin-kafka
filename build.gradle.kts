import kotlinx.knit.KnitPluginExtension
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
  dependencies {
    classpath("org.jetbrains.kotlinx:kotlinx-knit:0.4.0")
  }
}

@Suppress("DSL_SCOPE_VIOLATION")
plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.arrowGradleConfig.kotlin)
  alias(libs.plugins.arrowGradleConfig.nexus)
  alias(libs.plugins.arrowGradleConfig.publish)
  alias(libs.plugins.dokka)
}

apply(plugin = "kotlinx-knit")

allprojects {
  repositories {
    mavenCentral()
  }
  group = property("projects.group").toString()
  version = property("projects.version").toString()
  extra.set("dokka.outputDirectory", rootDir.resolve("docs"))
}

dependencies {
  api(libs.kotlin.stdlib)
  api(libs.kotlinx.coroutines.core)
  api(libs.kotlinx.coroutines.jdk8)
  api(libs.kafka.clients)
  implementation(libs.slf4j.api)
  
  testImplementation(libs.bundles.kotest)
  testImplementation(libs.testcontainers.kafka)
  testImplementation(libs.slf4j.simple)
}

configure<KnitPluginExtension> {
  siteRoot = "https://nomisrev.github.io/kotlin-kafka/"
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
  }
  
  withType<KotlinCompile>().configureEach {
    kotlinOptions.jvmTarget = "1.8"
  }
  
  register<Delete>("cleanDocs") {
    val folder = file("docs").also { it.mkdir() }
    val docsContent = folder.listFiles().filter { it != folder }
    delete(docsContent)
  }
}

nexusPublishing {
  repositories {
    named("sonatype") {
      nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
      snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
    }
  }
}
