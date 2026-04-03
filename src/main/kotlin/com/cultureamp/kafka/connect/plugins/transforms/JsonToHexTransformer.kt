package com.cultureamp.kafka.connect.plugins.transforms

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets

/**
 * A Kafka Connect transformer that converts schemaless JSON objects to hexadecimal format
 * for Redshift varbyte columns.
 *
 * This transformer takes the complete JSON payload and converts it to a single hexadecimal
 * string suitable for storage in Redshift varbyte columns. It handles schemaless JSON data
 * with nested structures up to 5MB in size.
 *
 * Features:
 * - Converts entire JSON object to a single hex string
 * - Handles schemaless JSON with nested structures
 * - Simple and lightweight transformation
 * - Configurable output field name
 *
 * Configuration:
 * - hex.field.name: Name of the field to store the hex-encoded JSON (default: "json_hex")
 *
 * @param R is ConnectRecord<R>.
 */
class JsonToHexTransformer<R : ConnectRecord<R>> : Transformation<R> {

    companion object {
        const val CONFIG_HEX_FIELD_NAME = "hex.field.name"
        private const val HEX_FIELD_NAME_DEFAULT = "json_hex"
    }

    private val logger = LoggerFactory.getLogger(this::class.java.canonicalName)
    private val objectMapper = ObjectMapper()
    private var hexFieldName: String = HEX_FIELD_NAME_DEFAULT

    override fun configure(configs: MutableMap<String, *>?) {
        hexFieldName = configs?.get(CONFIG_HEX_FIELD_NAME) as? String ?: HEX_FIELD_NAME_DEFAULT
        logger.info("Configured JsonToHexTransformer - hexFieldName: $hexFieldName")
    }

    override fun config(): ConfigDef {
        return ConfigDef()
            .define(
                CONFIG_HEX_FIELD_NAME,
                ConfigDef.Type.STRING,
                HEX_FIELD_NAME_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Name of the field to store the hex-encoded JSON payload"
            )
    }

    override fun close() {}

    override fun apply(record: R): R {
        try {
            return transformRecord(record)
        } catch (e: Exception) {
            logger.error("Exception processing record: ", e)
            logger.error("Record received: ${record.value()}")
            throw e
        }
    }

    /**
     * Converts a string to hexadecimal representation
     */
    private fun stringToHex(input: String): String {
        return input.toByteArray(StandardCharsets.UTF_8)
            .joinToString("") { "%02x".format(it) }
    }

    private fun convertFieldSchema(orig: Schema, optional: Boolean, defaultValue: Any?): Schema {
        val builder = SchemaUtil.copySchemaBasics(orig)
        if (optional)
            builder.optional()
        if (defaultValue != null)
            builder.defaultValue(defaultValue)
        return builder.build()
    }

    private fun transformRecord(record: R): R {
        val value = record.value()

        // Convert the value to JSON string
        val jsonString = when (value) {
            is String -> value
            is Map<*, *> -> objectMapper.writeValueAsString(value)
            else -> objectMapper.writeValueAsString(value)
        }

        // Convert JSON string to hex
        val hexValue = stringToHex(jsonString)

        // Create new schema with hex field and Kafka metadata fields
        val newSchema = SchemaBuilder.struct()
            .name("HexEncodedJson")
            .field(hexFieldName, Schema.STRING_SCHEMA)
            .field("_kafka_metadata_partition", convertFieldSchema(SchemaBuilder.int32().build(), false, null))
            .field("_kafka_metadata_offset", convertFieldSchema(SchemaBuilder.int64().build(), false, null))
            .field("_kafka_metadata_timestamp", convertFieldSchema(SchemaBuilder.int64().build(), false, null))
            .build()

        // Create new struct with values
        val newValue = Struct(newSchema)
            .put(hexFieldName, hexValue)
            .put("_kafka_metadata_partition", record.kafkaPartition())
            .put("_kafka_metadata_offset", (record as SinkRecord).kafkaOffset())
            .put("_kafka_metadata_timestamp", record.timestamp())

        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            newSchema,
            newValue,
            record.timestamp()
        ) as R
    }
}
