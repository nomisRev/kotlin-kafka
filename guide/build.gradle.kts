plugins {
  kotlin("jvm")
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(rootProject)
  implementation(libs.testcontainers.kafka)
  implementation("io.arrow-kt:suspendapp:2.2.3")
  testImplementation(kotlin("test-junit5"))
  testImplementation("org.jetbrains.kotlinx:kotlinx-knit-test:0.5.0")
  testImplementation("com.ginsberg:junit5-system-exit:2.0.3")
}

tasks.test {
  useJUnitPlatform()
  jvmArgumentProviders.add(CommandLineArgumentProvider {
    listOf("-javaagent:${configurations.testRuntimeClasspath.get().files.find {
      it.name.contains("junit5-system-exit") }
    }")
  })
}

sourceSets.test {
  java.srcDirs("example", "test")
}