enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")
enableFeaturePreview("VERSION_CATALOGS")

rootProject.name = "kotlin-kafka"

dependencyResolutionManagement {
  versionCatalogs {
    create("libs") {
      from(files("libs.versions.toml"))
    }
  }

  repositories {
    mavenCentral()
  }
}

include(":guide")
include("core")
