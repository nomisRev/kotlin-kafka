plugins {
  kotlin("jvm")
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(rootProject)
  implementation(libs.arrow.fx.coroutines)
  implementation(libs.testcontainers.kafka)
  testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
  testImplementation("org.jetbrains.kotlinx:kotlinx-knit-test:0.3.0")
}

sourceSets.test {
  java.srcDirs("example", "test")
}