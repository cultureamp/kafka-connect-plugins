val kafkaVersion = "3.6.2"

// Must track the log4j version in the cp-kafka-connect image (docker/connect/Dockerfile in
// kafka-ops). log4j2 plugins are loaded from a binary descriptor, so a major/minor mismatch
// here can mean the plugin is silently not found at runtime.
val log4jVersion = "2.25.3"

plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    id("org.jetbrains.kotlin.jvm") version "1.9.21"

    // Add ktlint
    id("org.jmailen.kotlinter") version "5.7.0"

    // Apply the java-library plugin for API and implementation separation.
    `java-library`

}

// Package version
version = "0.13.0"

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

    // log4j2, for PiiRedactionPolicy. compileOnly because the Connect worker classpath already
    // provides these - shipping our own copy risks two log4j versions at runtime.
    compileOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
    compileOnly("org.apache.logging.log4j:log4j-api:$log4jVersion")

    // Runs log4j2's PluginProcessor over src/main/java to generate
    // META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat. Without that
    // descriptor log4j2 cannot resolve <PiiRedactionPolicy>: it logs "Unable to invoke factory
    // method", then builds the Rewrite appender with NO policy and passes every event through
    // unredacted. javac runs this natively, so no kapt is needed.
    annotationProcessor("org.apache.logging.log4j:log4j-core:$log4jVersion")

    testImplementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
    testImplementation("org.apache.logging.log4j:log4j-api:$log4jVersion")
}

// PiiRedactionPolicy must ship SEPARATELY from the SMT jar, because the two have different
// deployment targets and different dependency budgets:
//
//   - SMTs      -> /usr/share/java/cultureamp-kafka-connect-plugins, on Connect's plugin.path,
//                  loaded later by a connector classloader that has kotlin-stdlib.
//   - log4j2    -> the Connect WORKER classpath, loaded at JVM startup by the system
//     policies      classloader. kafka-run-class puts only share/java/kafka and
//                  share/java/confluent-telemetry there, and kotlin-stdlib is on neither.
//
// So this jar contains only the Java logging package plus the generated plugin descriptor - no
// Kotlin classes, no kotlin_module, nothing that needs kotlin-stdlib at runtime. Verified by
// loading it in the cp-kafka-connect image with no Kotlin on the classpath.
val log4jRedactionJar by tasks.registering(Jar::class) {
    archiveBaseName.set("kafka-connect-log4j-redaction")
    from(sourceSets.main.get().output) {
        include("com/cultureamp/kafka/connect/plugins/logging/**")
        include("META-INF/org/apache/logging/log4j/core/config/plugins/**")
    }
}

// Keep the log4j policy OUT of the SMT jar. It is functionally harmless there - connector
// classloaders do not scan for log4j plugin descriptors - but including it means a
// logging-only change alters the SMT artifact's contents and checksum for no reason, and
// kafka-ops pins that artifact by MD5.
tasks.jar {
    exclude("com/cultureamp/kafka/connect/plugins/logging/**")
    exclude("META-INF/org/apache/logging/**")
    // log4j-core's annotation processor jar also runs GraalVmProcessor, which writes
    // META-INF/native-image/log4j-generated/<hash>/reflect-config.json. That is not under
    // META-INF/org/apache/logging, so the exclude above misses it and it lands in the SMT jar -
    // which is exactly the checksum churn this block exists to prevent. Useless to a JVM Connect
    // worker, so it is excluded from both jars rather than moved to the other one.
    exclude("META-INF/native-image/**")
}

tasks.named("assemble") {
    dependsOn(log4jRedactionJar)
}
