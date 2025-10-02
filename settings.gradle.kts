apply(from = "get-name-and-version.gradle.kts")

rootProject.name = gradle.extra["package_name"]!! as String

// Automatically downloads the JDK to use for compiling
plugins { id("org.gradle.toolchains.foojay-resolver-convention") version ("0.8.0") }
