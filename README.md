# Kafka Connect Plugins

A set of generic plugins for Kafka Connect that complement the built-in transformations, config providers, and connectors.

## ModifyPartition

Sets the partition number for a message based on a partition key stored in the message header.

This transform calculates the partition number by hashing the partition key, to get an integer, and then performs a modulo operation with the number of available partitions.

```
Hash(headers[header.key]) % number.partitions
```

### Configuration properties

|Name|Description|Type|Valid values|Importance|
|---|---|---|---|---|
|`header.key`|Key used to extract the partition key from the record headers.|string|Any key that is set as a header on the incoming record.|HIGH
|`number.partitions`|How many partitions does the topic have|int|-|HIGH

### Examples

Assume the following configuration:

```json
"transforms": "ModifyPartition",
"transforms.ModifyPartition.type":"com.cultureamp.kafka.connect.transforms.ModifyPartition",
"transforms.ModifyPartition.header.key": "account_id"
"transforms.ModifyPartition.number.partitions": "10"
```

Example

* Before: `Headers = { account_id: "04a96f30-3dfa-11ec-9bbc-0242ac130002" }, Partition = 0`
* After: `Headers = { account_id: "04a96f30-3dfa-11ec-9bbc-0242ac130002" }, Partition = 7`

## Unify Legacy SlackIntegration Payload

This is a custom transformer to unify `SlackIntegration` collections coming out of Murmur

We have 2 document variants for the `SlackIntegration` (Thanks MongoDB!)

**OAuth V1**
```
{
    "_id": {
        "$oid": "5e703178fff837001ffaf052"
    },
    "account_aggregate_id": "d8fb1ad0-6308-47c8-84d6-1eb0f367ca0d",
    "account_id": {
        "$oid": "5e2a754ddd99fd0022333044"
    },
    "created_at": "2020-03-17T02:10:00.181Z",
    "oauth_response_data": {
        "ok": true,
        "access_token": "xoxp-993008094547-1006686967558-1004086920580-f7723a0d45deb6a482350428918781e3",
        "scope": "identify,bot",
        "user_id": "U0106L6UFGE",
        "team_id": "TV7082SG3",
        "enterprise_id": null,
        "team_name": "Slack Testing",
        "bot": {
            "bot_user_id": "UV8DT789F",
            "bot_access_token": "xoxb-993008094547-994469246321-XgSvWxmFSfRHP8gf55Y7dS28"
        }
    },
    "status": "active",
    "updated_at": "2022-04-07T03:29:39.800Z",
    "v": 0
}
```
**OAuth V2**
```
{
    "_id": {
        "$oid": "624fd8c271efe5001fa984d5"
    },
    "account_aggregate_id": "8c6110c8-a879-40e7-9d3a-0fd91ee999a2",
    "account_id": {
        "$oid": "5ff62e4ce1cda00025f80576"
    },
    "created_at": "2022-04-08T06:40:02.649Z",
    "oauth_response_data": {
        "ok": true,
        "app_id": "A037SJ77B41",
        "authed_user": {
            "id": "U02L4T6TH42",
            "scope": "identity.basic,identity.email",
            "access_token": "xoxp-2681941652837-2684924935138-3363184295571-0912805e1a9fb49b9f36e070bfdea9cd",
            "token_type": "user"
        },
        "scope": "chat:write,commands,im:history,im:read,im:write,users.profile:read,users:read,users:read.email",
        "token_type": "bot",
        "access_token": "xoxb-2681941652837-3360300471349-0yNRFusOQ6ciye0X5AyDAB6S",
        "bot_user_id": "U03AL8UDVA9",
        "team": {
            "id": "T02L1TPK6QM",
            "name": "Angel CA Test Workspace"
        },
        "enterprise": null,
        "is_enterprise_install": false
    },
    "status": "active",
    "updated_at": "2022-04-08T06:40:02.657Z",
    "v": 0
}
```
What this transformer does is to make sure we can get a unified payload into the topic. Without a custom transformer we will have multiple attribute in the topic which will not make sense to anyone without context

### Examples

Assume the following configuration:

```json
"transforms": "UnifyLegacySlackIntegrationPayload",
"transforms.UnifyLegacySlackIntegrationPayload.type":"com.cultureamp.kafka.connect.transforms.UnifyLegacySlackIntegrationPayload",
```

Avro Schema:

```
{
  "type": "record",
  "name": "ConnectDefault",
  "namespace": "io.confluent.connect.avro",
  "fields": [
    {
      "name": "account_aggregate_id",
      "type": "string"
    },
    {
      "name": "access_token",
      "type": "string"
    },
    {
      "name": "team_id",
      "type": "string"
    },
    {
      "name": "team_name",
      "type": "string"
    }
  ]
}
```


## Installation
This library is built as a single `.jar` and published as a Github release. To install in your Connect cluster, add the JAR file to a directory that is on the clusters `plugin.path`.

#### Docker Example
```dockerfile
FROM confluentinc/cp-kafka-connect:6.2.0

ENV CULTUREAMP_CONNECT_PLUGIN_VERSION_TAG 0.1.0

RUN curl -LJO https://github.com/cultureamp/kafka-connect-plugins/releases/download/${CULTUREAMP_CONNECT_PLUGIN_VERSION_TAG}/kafka-connect-plugins-${CULTUREAMP_CONNECT_PLUGIN_VERSION_TAG}.jar && \
  mkdir /usr/share/java/cultureamp-connect-plugins/ && \ # Must be added to your `plugin.path`
  mv kafka-connect-plugins-${AWS_CONFIG_PROVIDER_VERSION_TAG}.jar /usr/share/java/cultureamp-connect-plugins/
```


## Development
This project is built using [Gradle][1]. To build the project run:
```
./gradlew build
```

This will create a jar:
```
./build/libs/kafka-connect-plugins-${version}.jar
```

### Useful commands
- `./gradlew test` Run all tests
- `./gradlew lintKotlin` Run Ktlint on all files
- `./gradlew formatKotlin` Fix any linting violations

- [1]: https://gradle.org/
