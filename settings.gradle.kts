enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

rootProject.name = "kotlin-kafka"

dependencyResolutionManagement {
  repositories {
    mavenCentral()
  }
}

include(":guide")
