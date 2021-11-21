import kotlinx.knit.KnitPluginExtension
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    dependencies {
        classpath("org.jetbrains.kotlinx:kotlinx-knit:0.3.0")
    }
}

@Suppress("DSL_SCOPE_VIOLATION")
plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.arrowGradleConfig.formatter)
    alias(libs.plugins.dokka)
}

apply(plugin = "kotlinx-knit")

group = "com.github.nomirev"
version = "1.0-SNAPSHOT"

allprojects {
    repositories {
        maven("https://oss.sonatype.org/content/repositories/snapshots/")
        mavenCentral()
    }
}

dependencies {
    api(libs.kotlin.stdlib)
    api(libs.kotlinx.coroutines.core)

    // Kafka, TODO split into separate modules (?)
    api(libs.kafka.clients)
    api(libs.kafka.streams)
    api(libs.kafka.connect)

    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotest.property)
    testImplementation(libs.kotest.framework)
    testImplementation(libs.kotest.assertions)
}

configure<KnitPluginExtension> {
    siteRoot = "https://nomisrev.github.io/KotlinKafka/"
}

tasks {
    withType<DokkaTask>().configureEach {
        outputDirectory.set(rootDir.resolve("docs"))
        moduleName.set("KotlinKafka")
        dokkaSourceSets {
            named("main") {
                includes.from("README.md")
                perPackageOption {
                    matchingRegex.set(".*\\.internal.*")
                    suppress.set(true)
                }
                sourceLink {
                    localDirectory.set(file("src/main/kotlin"))
                    remoteUrl.set(uri("https://github.com/nomisRev/KotlinKafka/tree/main/src/main/kotlin").toURL())
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
        sourceCompatibility = "1.8"
        targetCompatibility = "1.8"
    }
}
