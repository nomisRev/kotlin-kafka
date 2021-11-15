plugins {
    kotlin("jvm") version "1.5.31"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

allprojects {
    repositories {
        maven("https://oss.sonatype.org/content/repositories/snapshots/")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

dependencies {
    implementation(kotlin("stdlib"))
    // SNAPSHOT for resource DSL
    implementation("io.arrow-kt:arrow-core:1.0.2-SNAPSHOT")
    implementation("io.arrow-kt:arrow-fx-coroutines:1.0.2-SNAPSHOT")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.5.2")

    implementation("org.testcontainers:kafka:1.16.2")
    implementation("org.apache.kafka:kafka-clients:2.1.0")
    implementation("org.apache.kafka:kafka-streams:2.1.0")
    implementation("org.apache.kafka:connect-runtime:2.1.0")
//    implementation("io.confluent:kafka-json-serializer:5.0.1")

    testImplementation("io.kotest:kotest-runner-junit5:4.6.3")
    testImplementation("io.kotest:kotest-property:4.6.3")
    testImplementation("io.kotest:kotest-framework-engine:4.6.3")
    testImplementation("io.kotest:kotest-assertions-core:4.6.3")
}
