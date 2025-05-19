plugins {
  kotlin("jvm")
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(rootProject)
  implementation(libs.testcontainers.kafka)
  implementation("io.arrow-kt:suspendapp:2.1.2")
  testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
  testImplementation("org.jetbrains.kotlinx:kotlinx-knit-test:0.5.0")
}

sourceSets.test {
  java.srcDirs("example", "test")
}