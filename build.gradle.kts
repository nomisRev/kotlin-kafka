import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.arrowGradleConfig.formatter)
    alias(libs.plugins.dokka)
}

group = "com.github.nomirev"
version = "1.0-SNAPSHOT"

allprojects {
    repositories {
        maven("https://oss.sonatype.org/content/repositories/snapshots/")
        mavenCentral()
    }
}

dependencies {
    implementation(libs.kotlin.stdlib)
    implementation(libs.arrow.core)
    implementation(libs.arrow.fx.coroutines)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.coroutines.jdk8)

    implementation(libs.testcontainers.kafka)
    implementation(libs.kafka.clients)
    implementation(libs.kafka.streams)
    implementation(libs.kafka.connect)

    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotest.property)
    testImplementation(libs.kotest.framework)
    testImplementation(libs.kotest.assertions)
}

tasks {
    withType<DokkaTask>().configureEach {
        outputDirectory.set(rootDir.resolve("docs"))
        moduleName.set("Kotlin Kafka")
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

    withType<Test>().configureEach {
        useJUnitPlatform()
    }

    withType<KotlinCompile>().configureEach {
        kotlinOptions.jvmTarget = "1.8"
        sourceCompatibility = "1.8"
        targetCompatibility = "1.8"
    }
}
