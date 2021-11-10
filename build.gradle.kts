val kafkaVersion = "2.8.1"

plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    id("org.jetbrains.kotlin.jvm") version "1.5.0"

    // Add ktlint
    id("org.jmailen.kotlinter") version "3.6.0"

    // Apply the java-library plugin for API and implementation separation.
    `java-library`
}

// Package version
version = "0.1.0"

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    // Align versions of all Kotlin components
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))

    // Use the Kotlin JDK 8 standard library.
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    // Kafka dependencies
    implementation("org.apache.kafka:connect-api:$kafkaVersion")
    implementation("org.apache.kafka:connect-transforms:$kafkaVersion")

    // Use the Kotlin test library.
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    // Use the Kotlin JUnit integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
}
