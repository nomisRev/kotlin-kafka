plugins {
  kotlin("jvm")
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(rootProject)
  testImplementation(libs.arrow.fx.coroutines)
  testImplementation(libs.testcontainers.kafka)
  testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
  testImplementation("org.jetbrains.kotlinx:kotlinx-knit-test:0.2.3")
}

sourceSets.test {
  java.srcDirs("example", "test")
}