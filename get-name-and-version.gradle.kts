// File: get-name-and-version.gradle.kts
// Script that reads the package.json file and sets the package name and version as gradle
// properties
// Used in settings.gradle.kts to set the root project name and version
// Used in build.gradle.kts to set the project version
buildscript {
  repositories { mavenCentral() }
  dependencies { classpath("com.google.code.gson:gson:2.8.9") }
}

data class PackageJSON(var name: String, var version: String)

val gson = com.google.gson.Gson()
val packageJsonFile = file("package.json")
val packageJsonContent = gson.fromJson(packageJsonFile.readText(), PackageJSON::class.java)

gradle.extra["package_version"] = packageJsonContent.version

gradle.extra["package_name"] = packageJsonContent.name
