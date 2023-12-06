# Deployment Guide for Kafka Connect Plugins

This guide walks you through the steps to deploy our Kafka Connect plugins, release a new version of the plugins and how to update that version in kafka-ops.

## Prerequisites

- Ensure you have GitHub [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens) with full access to Culture Amp private repos

## Deployment Steps

### 1. Make sure tests, linter and dependencies' vulnerability checks are passing

Run the following commands:
```
./gradlew test
./gradlew lintKotlin
./gradlew dependencyCheckAnalyze
```

### 2. Update package version in `build.gradle.kts`

Update this line in the aforementioned file
```
// Package version
version = "X.X.X"
```

### 3. Create build

This project is built using Gradle. To build the project run:
```
./gradlew build
```

This will create a jar:
```
./build/libs/kafka-connect-plugins-${version}.jar
```

Create [md5sum hash](https://www.md5hashgenerator.com/) by running:
```
md5sum kafka-connect-plugins-X.X.X.jar
```

Save that output because it will be used to update correspondent value in kafka-ops dockerfile [further](#5-update-kafka-connect-plugins-version-in-kafka-ops)

## Releasing a New Version

### 1. Tag the Release

After successful deployment, tag the release using the following commands:

```
git tag -a vX.X.X -m "Release message"
```

Replace `vX.X.X` with your version number and `"Release message"` with your release message.

### 2. Push the tag

Push the tag to the remote repository:

```
git push origin vX.X.X
```

Again, replace `vX.X.X` with your version number.

### 3. Create release in GitHub repo of [kafka-connect-plugins](https://github.com/cultureamp/kafka-connect-plugins/releases/new)

Prepare release notes summarizing the changes in this version. Please note if release contains any breaking changes.

## Continue with the deployment steps

### 4. Merge your PR

As above.

### 5. Update kafka-connect-plugins version in kafka-ops

Create new kafka-ops PR with changes in [containers/connect/Dockerfile](https://github.com/cultureamp/kafka-ops/blob/main/containers/connect/Dockerfile). Those changes should include: `CA_PLUGINS_VERSION` env version bump and CA_PLUGINS_MD5SUM env should now include that md5sum hash we generated [earlier](#3-create-build)

### 6. Merge kafka-ops PR

As above.

### 7. Deploy kafka-ops-platform-cluster and kafka-ops-platform-services in  [kafka-ops-infrastructure](https://buildkite.com/culture-amp/kafka-ops-infrastructure/)

Merge of the kafka-ops PR in the previous step will kick-off the pipeline, however you will need to manually deploy master in the following order:

1. Find the branch for the kafka-ops PR and deploy it to `development-us` env
2. Deploy to `Production-US`
3. Deploy to `Production-EU`
4. Deploy to `Staging-US`

### 8. Post-deploy steps

Verify cluster health by looking at the [Kafka Cluster](https://app.datadoghq.com/dashboard/nu9-eun-dpj/kafka-cluster-overview?refresh_mode=sliding&from_ts=1694325305136&to_ts=1696917305136&live=true) and [Connect Cluster](https://app.datadoghq.com/dashboard/nny-trn-iq6/kafka-connect-summary-v2?refresh_mode=sliding&from_ts=1696312530072&to_ts=1696917330072&live=true) Dashboards and announce the release in #kafka_announcements channel
