val kafkaVersion = "3.6.2"
val javaVersion = "17"

plugins {
  // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
  kotlin("jvm") version "1.9.22"

    // Add ktlint
    id("org.jmailen.kotlinter") version "3.6.0"

    // Apply the java-library plugin for API and implementation separation.
    `java-library`

  // Enable publishing to GitHub Maven repository
  `maven-publish`
}


group = "com.cultureamp"
// Package version
version = gradle.extra["package_version"]!! as String

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
    // CVE-2024-27309 https://security.snyk.io/vuln/SNYK-JAVA-ORGAPACHEKAFKA-6600922
    implementation("org.apache.kafka:connect-api:$kafkaVersion")
    implementation("org.apache.kafka:connect-json:$kafkaVersion")
    implementation("org.apache.kafka:connect-transforms:$kafkaVersion")
    implementation("org.apache.avro:avro:1.11.3")

    // Use the Kotlin JUnit integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testRuntimeOnly("org.junit.vintage:junit-vintage-engine:5.10.0")

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
    implementation("org.apache.commons:commons-compress:1.28.0")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.3")
    implementation("org.mongodb.kafka:mongo-kafka-connect:1.7.0")
    implementation("org.mongodb:bson:4.5.1")
}

// Set target java version
configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
  jvmToolchain { languageVersion.set(JavaLanguageVersion.of(javaVersion)) }
}

java {
  toolchain { languageVersion.set(JavaLanguageVersion.of(javaVersion)) }

  // publish source jar to maven
  withSourcesJar()
}

publishing {
  repositories {
    maven {
      name = "GitHubPackages"
      url = uri("https://maven.pkg.github.com/cultureamp/${rootProject.name}")
      credentials {
        username = System.getenv("USERNAME")
        password = System.getenv("PACKAGE_WRITE_TOKEN")
      }
    }
  }
  publications {
    create<MavenPublication>("maven") {
      artifactId = project.name
      from(components["java"])
    }
  }
}

tasks.named<Test>("test") { 
    useJUnitPlatform()
}

tasks.jar {
  manifest {
    attributes(
      mapOf(
        "Implementation-Title" to project.name,
        "Implementation-Version" to project.version,
      ),
    )
  }

  archiveBaseName.set(project.name)
}
