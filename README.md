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
