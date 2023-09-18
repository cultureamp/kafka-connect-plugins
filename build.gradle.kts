val kafkaVersion = "3.4.0"

plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    id("org.jetbrains.kotlin.jvm") version "1.6.10"

    // Add ktlint
    id("org.jmailen.kotlinter") version "3.6.0"

    // Vulnerable dependency checker
    id("org.owasp.dependencycheck") version "8.3.1"

    // Apply the java-library plugin for API and implementation separation.
    `java-library`
}

// Package version
version = "0.7.1-alpha"

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
    implementation("org.apache.kafka:connect-json:$kafkaVersion")
    implementation("org.apache.kafka:connect-transforms:$kafkaVersion")
    implementation("org.apache.avro:avro:1.11.1")

    // Use the Kotlin test library.
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    // Use the Kotlin JUnit integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")

    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("ch.qos.logback:logback-core:1.2.11")

    // Upgraded version of Jackson Databind to patch:
    // CVE-2022-42003 - https://github.com/advisories/GHSA-jjjh-jjxp-wpff
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.4.2")

    // Upgraded version of Snappy Java to patch: 
    // CVE-2023-34454 - https://github.com/advisories/GHSA-fjpj-2g6w-x25r
    // CVE-2023-34453 - https://github.com/advisories/GHSA-pqr6-cmr2-h8hf
    // CVE-2023-34455 - https://github.com/advisories/GHSA-qcwq-55hx-v3vh
    implementation("org.xerial.snappy:snappy-java:1.1.10.1")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.3")
    implementation("org.mongodb.kafka:mongo-kafka-connect:1.7.0")
    implementation("org.mongodb:bson:4.5.1")
}

//A full list of config options can be found here:
//https://jeremylong.github.io/DependencyCheck/dependency-check-gradle/configuration.html
dependencyCheck {
    // anything over a 5.0 is above a 'warning'
    failBuildOnCVSS = 5.0F
    analyzers.assemblyEnabled = false
}

