val kafkaVersion = "3.6.2"

// Must track the log4j version in the cp-kafka-connect image (docker/connect/Dockerfile in
// kafka-ops). log4j2 plugins are loaded from a binary descriptor, so a major/minor mismatch
// here can mean the plugin is silently not found at runtime.
val log4jVersion = "2.25.3"

plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    id("org.jetbrains.kotlin.jvm") version "1.9.21"

    // Runs log4j2's PluginProcessor over the Kotlin sources to generate
    // META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat. Without this
    // descriptor, log4j2 cannot resolve <PiiRedactionPolicy> and the whole logging config fails
    // to parse - which falls back to DefaultConfiguration and logs full messages to the console.
    id("org.jetbrains.kotlin.kapt") version "1.9.21"

    // Add ktlint
    id("org.jmailen.kotlinter") version "3.6.0"

    // Apply the java-library plugin for API and implementation separation.
    `java-library`

}

// Package version
version = "0.12.1"

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
    // Previous 3.6.0 version was flagged as vulnerability:
    // CVE-2024-27309
    implementation("org.apache.kafka:connect-api:$kafkaVersion")
    implementation("org.apache.kafka:connect-json:$kafkaVersion")
    implementation("org.apache.kafka:connect-transforms:$kafkaVersion")
    implementation("org.apache.avro:avro:1.11.3")

    // Use the Kotlin test library.
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    // Use the Kotlin JUnit integration.
    testRuntimeOnly("org.junit.jupiter:junit-jupiter:5.10.0")

    // CVE-2023-6378 https://logback.qos.ch/news.html#1.3.12
    implementation("ch.qos.logback:logback-classic:1.4.14")
    implementation("ch.qos.logback:logback-core:1.4.14")

    // Previous 2.15.2 version was flagged as vulnerability:
    // CVE-2023-35116 - developers claim it's a bogus alert https://github.com/FasterXML/jackson-databind/issues/3972
    // but I guess won't hurt to upgrade it + will resolve dependency check failure
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.0")

    // Upgraded version of Snappy Java to patch:
    // CVE-2023-34454 - https://github.com/advisories/GHSA-fjpj-2g6w-x25r
    // CVE-2023-34453 - https://github.com/advisories/GHSA-pqr6-cmr2-h8hf
    // CVE-2023-34455 - https://github.com/advisories/GHSA-qcwq-55hx-v3vh
    implementation("org.xerial.snappy:snappy-java:1.1.10.5")

    // CVE-2023-42503
    implementation("org.apache.commons:commons-compress:1.26.0")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.3")
    implementation("org.mongodb.kafka:mongo-kafka-connect:1.7.0")
    implementation("org.mongodb:bson:4.5.1")

    // log4j2, for PiiRedactionPolicy. compileOnly because the Connect image already provides
    // these on the classpath - shipping our own copy risks two log4j versions at runtime.
    compileOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
    compileOnly("org.apache.logging.log4j:log4j-api:$log4jVersion")
    kapt("org.apache.logging.log4j:log4j-core:$log4jVersion")
    testImplementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
    testImplementation("org.apache.logging.log4j:log4j-api:$log4jVersion")
}
