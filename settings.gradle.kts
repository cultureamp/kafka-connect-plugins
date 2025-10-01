rootProject.name = "kafka-connect-plugins"
// Set up to download gradle plugins from GitHub Packages
pluginManagement {
  repositories {
    maven {
      name = "GitHubPackages"
      url = uri("https://maven.pkg.github.com/cultureamp/*")
      credentials {
        username = System.getenv("USERNAME") ?: extra["gpr.user"] as String
        password = System.getenv("PACKAGE_READ_TOKEN") ?: extra["gpr.key"] as String
      }
    }
    gradlePluginPortal()
  }
}

apply(from = "get-name-and-version.gradle.kts")

rootProject.name = gradle.extra["package_name"]!! as String
