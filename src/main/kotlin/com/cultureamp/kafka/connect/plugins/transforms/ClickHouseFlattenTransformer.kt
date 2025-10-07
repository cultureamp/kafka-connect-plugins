package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.slf4j.LoggerFactory
import java.util.Collections

/**
 * A ClickHouse-optimized flatten transformer that preserves native array and map types.
 *
 * This transformer flattens nested structures while preserving the semantic meaning of complex data types.
 * Unlike standard flatten transformers that convert arrays/maps to JSON strings, this transformer
 * maintains native arrays and maps, which is optimal for ClickHouse's columnar storage.
 *
 * Features:
 * - Flattens nested object structures (body.field -> body_field)
 * - Preserves arrays as native arrays (not JSON strings)
 * - Preserves maps as native maps (not JSON strings)
 * - Adds Kafka metadata fields (_kafka_metadata_*)
 * - Handles tombstone records with is_deleted flag
 *
 * @param R is ConnectRecord<R>.
 * @constructor Creates a ClickHouseFlattenTransformer Transformation<R> for a given ConnectRecord<T>
 */
class ClickHouseFlattenTransformer<R : ConnectRecord<R>> : Transformation<R> {
    private val logger = LoggerFactory.getLogger(this::class.java.canonicalName)
    private val purpose = "ClickHouse™ Flatten Transform with Native Type Preservation"
    private val schemaUpdateCache = SynchronizedCache<Schema, Schema>(LRUCache<Schema, Schema>(16))

    override fun configure(configs: MutableMap<String, *>?) {}

    override fun config(): ConfigDef {
        return ConfigDef()
    }

    override fun close() {}

    override fun apply(record: R): R {
        try {
            return targetPayload(record)
        } catch (e: Exception) {
            logger.error("Exception: ", e)
            logger.error("Record Received: " + record.value())
            throw e
        }
    }

    private fun newRecord(record: R, schema: Schema, value: Struct?): R {
        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            schema,
            value,
            record.timestamp()
        )
    }

    private fun fieldName(prefix: String, fieldName: String): String {
        if (prefix.isEmpty()) {
            return fieldName
        } else {
            return (prefix + '_' + fieldName)
        }
    }

    private fun convertFieldSchema(orig: Schema, optional: Boolean, defaultFromParent: Any?): Schema {
        // Note that we don't use the schema translation cache here. It might save us a bit of effort, but we really
        // only care about caching top-level schema translations.
        val builder = SchemaUtil.copySchemaBasics(orig)
        if (optional)
            builder.optional()
        if (defaultFromParent != null)
            builder.defaultValue(defaultFromParent)
        return builder.build()
    }

    private fun convertComplexFieldSchema(orig: Schema, optional: Boolean): Schema {
        return when (orig.type()) {
            Schema.Type.ARRAY -> {
                val builder = SchemaBuilder.array(orig.valueSchema())
                if (optional) builder.optional()
                builder.build()
            }
            Schema.Type.MAP -> {
                val builder = SchemaBuilder.map(orig.keySchema(), orig.valueSchema())
                if (optional) builder.optional()
                builder.build()
            }
            else -> {
                val builder = SchemaUtil.copySchemaBasics(orig)
                if (optional) builder.optional()
                builder.build()
            }
        }
    }

    public fun buildUpdatedSchema(schema: Schema, fieldNamePrefix: String, newSchema: SchemaBuilder, optional: Boolean) {
        for (field in schema.fields()) {
            val fieldName = fieldName(fieldNamePrefix, field.name())
            val fieldDefaultValue = if (field.schema().defaultValue() != null) {
                field.schema().defaultValue()
            } else if (schema.defaultValue() != null) {
                val checkParent = schema.defaultValue() as Struct
                checkParent.get(field)
            } else {
                null
            }
            when (field.schema().type()) {
                Schema.Type.INT8 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), optional, fieldDefaultValue))
                Schema.Type.INT16 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), optional, fieldDefaultValue))
                Schema.Type.INT32 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), optional, fieldDefaultValue))
                Schema.Type.INT64 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), optional, fieldDefaultValue))
                Schema.Type.FLOAT32 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), optional, fieldDefaultValue))
                Schema.Type.FLOAT64 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), optional, fieldDefaultValue))
                Schema.Type.BOOLEAN -> newSchema.field(fieldName, convertFieldSchema(field.schema(), optional, fieldDefaultValue))
                Schema.Type.STRING -> newSchema.field(fieldName, convertFieldSchema(field.schema(), optional, fieldDefaultValue))
                Schema.Type.BYTES -> newSchema.field(fieldName, convertFieldSchema(field.schema(), optional, fieldDefaultValue))
                // ARRAY and MAP keep their original types, no conversion to string
                Schema.Type.ARRAY -> newSchema.field(fieldName, convertComplexFieldSchema(field.schema(), optional))
                Schema.Type.MAP -> newSchema.field(fieldName, convertComplexFieldSchema(field.schema(), optional))
                Schema.Type.STRUCT -> buildUpdatedSchema(field.schema(), fieldName, newSchema, optional)
                else -> throw DataException(
                    "Flatten transformation does not support " + field.schema().type() +
                        " for record with schemas (for field " + fieldName + ")."
                )
            }
        }
    }

    private fun buildWithSchema(sourceRecord: Any?, fieldNamePrefix: String, newRecord: Struct) {
        if (sourceRecord == null) {
            return
        }
        val record = sourceRecord as Struct

        if (record.schema() == null) {
            return
        }

        for (field in record.schema().fields()) {
            val fieldName = fieldName(fieldNamePrefix, field.name())
            var value = record.get(field)
            if (value == null && field.schema().defaultValue() != null) {
                value = field.schema().defaultValue()
            }
            when (field.schema().type()) {
                Schema.Type.INT8 -> newRecord.put(fieldName, value)
                Schema.Type.INT16 -> newRecord.put(fieldName, value)
                Schema.Type.INT32 -> newRecord.put(fieldName, value)
                Schema.Type.INT64 -> newRecord.put(fieldName, value)
                Schema.Type.FLOAT32 -> newRecord.put(fieldName, value)
                Schema.Type.FLOAT64 -> newRecord.put(fieldName, value)
                Schema.Type.BOOLEAN -> newRecord.put(fieldName, value)
                Schema.Type.STRING -> newRecord.put(fieldName, value)
                Schema.Type.BYTES -> newRecord.put(fieldName, value)
                Schema.Type.ARRAY -> newRecord.put(fieldName, value)
                Schema.Type.MAP -> newRecord.put(fieldName, value)
                Schema.Type.STRUCT -> buildWithSchema(record.getStruct(field.name()), fieldName, newRecord)
                else -> throw DataException(
                    "Flatten transformation does not support " + field.schema().type() +
                        " for record with schemas (for field " + fieldName + ")."
                )
            }
        }
    }

    private fun targetPayload(record: R): R {
        val sourceValue = Requirements.requireStructOrNull(record.value(), purpose)
        val sourceSchema = record.valueSchema()
        var updatedSchema = schemaUpdateCache.get(sourceSchema)
        val props = Collections.singletonMap("schemas.enable", false)
        jsonConverter.configure(props, true)
        if (updatedSchema == null) {
            var builder: SchemaBuilder = SchemaUtil.copySchemaBasics(SchemaBuilder.struct())
            if (sourceSchema != null) {
                builder = SchemaUtil.copySchemaBasics(sourceSchema, SchemaBuilder.struct())
                // fix optional to true to prevent issues with is_deleted messages
                // CREATE TABLE queries should be created with other fields as NULL except for record key and is_deleted as false
                buildUpdatedSchema(sourceSchema, "", builder, true)
            }
            builder.field("topic_key", convertFieldSchema(SchemaBuilder.string().build(), false, ""))
            builder.field("is_deleted", convertFieldSchema(SchemaBuilder.int32().build(), false, 0))
            builder.field("_kafka_metadata_partition", convertFieldSchema(SchemaBuilder.string().build(), true, null))
            builder.field("_kafka_metadata_offset", convertFieldSchema(SchemaBuilder.string().build(), true, null))
            builder.field("_kafka_metadata_timestamp", convertFieldSchema(SchemaBuilder.int64().build(), true, null))
            updatedSchema = builder.build()
            schemaUpdateCache.put(sourceSchema, updatedSchema)
        }
        val updatedValue = Struct(updatedSchema)
        updatedValue.put("_kafka_metadata_partition", record.kafkaPartition().toString())
        if (record is SinkRecord) {
            updatedValue.put("_kafka_metadata_timestamp", record.timestamp())
            updatedValue.put("_kafka_metadata_offset", record.kafkaOffset().toString())
        }

        if (record.key() != null) {
            updatedValue.put("topic_key", record.key().toString())
        }
        if (sourceValue != null) {
            updatedValue.put("is_deleted", 0)
            buildWithSchema(sourceValue, "", updatedValue)
        }
        val bodyStruct = sourceValue?.getStruct("body")
        val deletedAt = bodyStruct?.get("deleted_at")
        if (sourceValue == null || bodyStruct == null || deletedAt != null) {
            updatedValue.put("is_deleted", 1)
        }
        return newRecord(record, updatedSchema, updatedValue)
    }

    private val jsonConverter = JsonConverter()
}
