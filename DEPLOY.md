# Deployment Guide for Kafka Connect Plugins

This guide walks you through the steps to deploy our Kafka Connect plugins, release a new version of the plugins and how to update that version in kafka-ops.

## Releasing a New Version

### Changesets details

This repository is set up with [Changesets](https://github.com/changesets/changesets) for release management.

For details of how to use it, see our [quick start guide to Changesets](https://cultureamp.atlassian.net/wiki/spaces/DE/pages/4442882075/Changesets+A+quick+start+guide)

### 1. Create a changeset with `changeset` command

### 2. Merge your PR

Changesets will automatically create a new release PR. This PR will include the version bump and changelog updates.

### 3. Review and merge the release PR

## Updating `kafka-ops` to use the new version
### 4. Update kafka-connect-plugins version in kafka-ops

Create new kafka-ops PR with changes in [containers/connect/Dockerfile](https://github.com/cultureamp/kafka-ops/blob/main/containers/connect/Dockerfile). Those changes should include: `CA_PLUGINS_VERSION` env version bump and CA_PLUGINS_MD5SUM env should now include the md5sum from the [github release](https://github.com/cultureamp/kafka-connect-plugins/releases)

### 5. Merge kafka-ops PR

As above.

### 6. Deploy kafka-ops-platform-cluster and kafka-ops-platform-services in  [kafka-ops-infrastructure](https://buildkite.com/culture-amp/kafka-ops-infrastructure/)

Merge of the kafka-ops PR in the previous step will kick-off the pipeline, however you will need to manually deploy master in the following order:

1. Find the branch for the kafka-ops PR and deploy it to `development-us` env
2. Deploy to `Production-US`
3. Deploy to `Production-EU`
4. Deploy to `Staging-US`

### 7. Post-deploy steps

Verify cluster health by looking at the [Kafka Cluster](https://app.datadoghq.com/dashboard/nu9-eun-dpj/kafka-cluster-overview?refresh_mode=sliding&from_ts=1694325305136&to_ts=1696917305136&live=true) and [Connect Cluster](https://app.datadoghq.com/dashboard/nny-trn-iq6/kafka-connect-summary-v2?refresh_mode=sliding&from_ts=1696312530072&to_ts=1696917330072&live=true) Dashboards and announce the release in #kafka_announcements channel
