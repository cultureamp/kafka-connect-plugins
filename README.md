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
"transforms.ModifyPartition.type":"com.cultureamp.kafka.connect.plugins.transforms.ModifyPartition",
"transforms.ModifyPartition.header.key": "account_id"
"transforms.ModifyPartition.number.partitions": "10"
```

Example

* Before: `Headers = { account_id: "04a96f30-3dfa-11ec-9bbc-0242ac130002" }, Partition = 0`
* After: `Headers = { account_id: "04a96f30-3dfa-11ec-9bbc-0242ac130002" }, Partition = 7`

## SlackIntegration Payload Transformer

This is a custom transformer to extract and reshape fields from `SlackIntegration` collections coming out of Murmur.

There are 2 document variants for the `SlackIntegration`
- OAuth V1: `resources/com/cultureamp/slack-integration-insert-v1.json`
- OAuth V2: `resources/com/cultureamp/slack-integration-insert-v2.json`

What this transformer does is to make sure we can get a unified payload into the topic. Without a custom transformer we will have multiple attributes in the topic which will not make sense to anyone without context.

### Examples
Assume the following configuration:

```yaml
"transforms": "SlackIntegrationPayloadTransformer"
"transforms.SlackIntegrationPayloadTransformer.type":"com.cultureamp.kafka.connect.transforms.SlackIntegrationPayloadTransformer"
```

Target Avro Schema: `resources/com/cultureamp/slack-integration-target-schema.avsc`

## JsonToHexTransformer

Converts schemaless JSON objects to hexadecimal format for storage in Redshift varbyte columns. This transformer takes the complete JSON payload (including nested fields) and converts it to a single hexadecimal string, making it suitable for Redshift varbyte columns which can handle up to 5MB of data.

### Use Case
When you have JSON data with nested structures that need to be stored in Redshift but don't want to flatten or lose the original structure, this transformer allows you to store the complete JSON as a hex-encoded string in a varbyte column. This is particularly useful for:
- Complex nested JSON structures
- JSON data that might have schema variations
- Preserving complete JSON payloads for later processing

### Configuration properties

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`hex.field.name`|Name of the field to store the hex-encoded JSON payload|string|`json_hex`|MEDIUM|

### Examples

Assume the following configuration:

```json
"transforms": "JsonToHex",
"transforms.JsonToHex.type": "com.cultureamp.kafka.connect.plugins.transforms.JsonToHexTransformer",
"transforms.JsonToHex.hex.field.name": "json_payload_hex"
```

Example transformation:

**Before:**
```json
{
  "user": {
    "name": "John Doe",
    "details": {
      "age": 30,
      "city": "New York",
      "preferences": ["coding", "reading"]
    }
  },
  "metadata": {
    "timestamp": "2023-01-01T00:00:00Z",
    "version": 1
  }
}
```

**After:**
```json
{
  "json_payload_hex": "7b2275736572223a7b226e616d65223a224a6f686e20446f65222c2264657461696c73223a7b22616765223a33302c2263697479223a224e657720596f726b222c22707265666572656e636573223a5b22636f64696e67222c227265616"
}
```

The hex string can then be stored in a Redshift varbyte column and decoded back to the original JSON when needed.

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

### For deployment instructions, please refer to [the deployment guide](DEPLOY.md)

[1]: https://gradle.org/
