plugins {
  kotlin("multiplatform")
}

repositories {
  mavenCentral()
}

kotlin {
  jvm()
  val nativeTargets = listOf(linuxX64(), mingwX64(), macosX64())

  sourceSets {
    val commonMain by getting {
      dependencies {
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
      }
    }
    val commonTest by getting {
      dependencies {
        implementation("io.kotest:kotest-assertions-core:5.5.4")
        implementation("io.kotest:kotest-framework-engine:5.5.4")
        implementation("io.kotest:kotest-property:5.5.4")
      }
    }
    val jvmMain by getting {
      dependencies {
        api(libs.kafka.clients)
        implementation(libs.slf4j.api)
      }
    }
    val jvmTest by getting {
      dependencies {
        implementation("io.kotest:kotest-runner-junit5-jvm:5.4.0")
      }
    }
    val nativeMain by creating {
      dependsOn(commonMain)
      dependencies {
        implementation("com.icemachined:kafka-client:0.2.0")
      }
    }
    val nativeTest by creating {
      dependsOn(commonTest)
    }

    nativeTargets.forEach {
      getByName("${it.name}Main").dependsOn(nativeMain)
    }
  }
}
