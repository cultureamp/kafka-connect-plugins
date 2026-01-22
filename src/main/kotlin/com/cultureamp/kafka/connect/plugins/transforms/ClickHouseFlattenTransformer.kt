package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil

/**
 * A ClickHouse-optimized flatten transformer that preserves native array and map types.
 *
 * This transformer flattens nested structures while preserving the semantic meaning of complex data types.
 * Unlike standard flatten transformers that convert arrays/maps to JSON strings, this transformer
 * maintains native arrays and maps, which is optimal for ClickHouse's columnar storage.
 *
 * Features:
 * - Flattens the top-level nested object structures (body.field -> body_field)
 * - Lower-level nested object structures are preserved as their composite types in composite columns (`body.array_field` -> `body_array_field` - type `Array` - `body.composite_field` -> `body_composite_field` - Type `Tuple` or `Nested`)
 * - Preserves arrays as native arrays (not JSON strings)
 * - Preserves maps as native maps (not JSON strings)
 * - Adds Kafka metadata fields (_kafka_metadata_*)
 * - Handles deleted records with is_deleted flag
 *
 * @param R is ConnectRecord<R>.
 * @constructor Creates a ClickHouseFlattenTransformer Transformation<R> for a given ConnectRecord<T>
 */
class ClickHouseFlattenTransformer<R : ConnectRecord<R>> : Transformation<R> {
    private val purpose = "ClickHouse™ Flatten Transform with Native Type Preservation"
    private val schemaUpdateCache = SynchronizedCache<Schema, Schema>(LRUCache<Schema, Schema>(16))

    override fun configure(configs: MutableMap<String, *>?) {}

    override fun config(): ConfigDef {
        return ConfigDef()
    }

    override fun close() {}

    override fun apply(record: R): R {
        return targetPayload(record)
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

    private fun fieldName(prefix: String, fieldName: String): String =
        if (prefix.isEmpty()) {
            fieldName
        } else {
            (prefix + '_' + fieldName)
        }

    private fun convertFieldSchema(orig: Schema, optional: Boolean, defaultFromParent: Any?): Schema {
        // Note that we don't use the schema translation cache here. It might save us a bit of effort, but we really
        // only care about caching top-level schema translations.
        val builder = SchemaUtil.copySchemaBasics(orig)
        if (optional)
            builder.optional()
        if (defaultFromParent != null)
            builder.defaultValue(defaultFromParent)
        else if (orig.defaultValue() != null)
            builder.defaultValue(orig.defaultValue())
        return builder.build()
    }

    private fun convertComplexFieldSchema(orig: Schema, optional: Boolean): Schema {
        return when (orig.type()) {
            Schema.Type.ARRAY -> {
                SchemaBuilder.array(orig.valueSchema())
            }
            Schema.Type.MAP -> {
                SchemaBuilder.map(orig.keySchema(), orig.valueSchema())
            }
            else -> {
                SchemaUtil.copySchemaBasics(orig)
            }
        }.let { builder ->
            if (optional) builder.optional()
            builder.defaultValue(orig.defaultValue())
            builder.build()
        }
    }

    fun buildUpdatedSchema(schema: Schema, fieldNamePrefix: String, newSchema: SchemaBuilder, optional: Boolean) {
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
                Schema.Type.INT8,
                Schema.Type.INT16,
                Schema.Type.INT32,
                Schema.Type.INT64,
                Schema.Type.FLOAT32,
                Schema.Type.FLOAT64,
                Schema.Type.BOOLEAN,
                Schema.Type.STRING,
                Schema.Type.BYTES -> newSchema.field(fieldName, convertFieldSchema(field.schema(), optional, fieldDefaultValue))
                // ARRAY and MAP keep their original types, no conversion to string
                Schema.Type.ARRAY,
                Schema.Type.MAP -> newSchema.field(fieldName, convertComplexFieldSchema(field.schema(), optional))
                Schema.Type.STRUCT -> buildUpdatedSchema(field.schema(), fieldName, newSchema, optional)
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
                Schema.Type.INT8,
                Schema.Type.INT16,
                Schema.Type.INT32,
                Schema.Type.INT64,
                Schema.Type.FLOAT32,
                Schema.Type.FLOAT64,
                Schema.Type.BOOLEAN,
                Schema.Type.STRING,
                Schema.Type.BYTES,
                Schema.Type.ARRAY,
                Schema.Type.MAP -> newRecord.put(fieldName, value)
                Schema.Type.STRUCT -> buildWithSchema(record.getStruct(field.name()), fieldName, newRecord)
            }
        }
    }

    /**
     * Populates default values for fields that haven't been set in the target struct.
     * This is used for delete events where the source record or body is null,
     * ensuring non-nullable fields have values to satisfy the ClickHouse connector.
     *
     * Only populates fields that are non-optional in the source schema, as optional
     * fields can remain unset (null).
     *
     * @param sourceSchema The source Avro schema to read field definitions from
     * @param targetStruct The target struct to populate defaults into
     * @param fieldNamePrefix The prefix for flattened field names (e.g., "body")
     */
    private fun populateDefaultsForNonOptionalFields(sourceSchema: Schema, targetStruct: Struct, fieldNamePrefix: String) {
        for (field in sourceSchema.fields()) {
            val flattenedFieldName = fieldName(fieldNamePrefix, field.name())

            when (field.schema().type()) {
                Schema.Type.STRUCT -> {
                    // Only recurse into non-optional structs
                    // Optional structs can remain null entirely
                    if (!field.schema().isOptional) {
                        populateDefaultsForNonOptionalFields(field.schema(), targetStruct, flattenedFieldName)
                    }
                }
                Schema.Type.INT8,
                Schema.Type.INT16,
                Schema.Type.INT32,
                Schema.Type.INT64,
                Schema.Type.FLOAT32,
                Schema.Type.FLOAT64,
                Schema.Type.BOOLEAN,
                Schema.Type.STRING,
                Schema.Type.BYTES,
                Schema.Type.ARRAY,
                Schema.Type.MAP -> {
                    // Only populate if the field exists in target schema and is non-optional in source
                    val targetField = targetStruct.schema().field(flattenedFieldName)
                    if (targetField != null && !field.schema().isOptional) {
                        // Use schema default if available, otherwise use type-specific default
                        val defaultValue = field.schema().defaultValue() ?: getTypeDefault(field.schema().type())
                        targetStruct.put(flattenedFieldName, defaultValue)
                    }
                }
            }
        }
    }

    /**
     * Returns a sensible default value for the given schema type.
     * Used when a field has no schema-defined default but needs a value.
     */
    private fun getTypeDefault(type: Schema.Type): Any? {
        return when (type) {
            Schema.Type.STRING -> ""
            Schema.Type.INT8 -> 0.toByte()
            Schema.Type.INT16 -> 0.toShort()
            Schema.Type.INT32 -> 0
            Schema.Type.INT64 -> 0L
            Schema.Type.FLOAT32 -> 0.0f
            Schema.Type.FLOAT64 -> 0.0
            Schema.Type.BOOLEAN -> false
            Schema.Type.BYTES -> ByteArray(0)
            Schema.Type.ARRAY -> emptyList<Any>()
            Schema.Type.MAP -> emptyMap<String, Any>()
            else -> null
        }
    }

    private fun targetPayload(record: R): R {
        val sourceValue = Requirements.requireStructOrNull(record.value(), purpose)
        val sourceSchema = record.valueSchema()
        var updatedSchema = schemaUpdateCache.get(sourceSchema)
        if (updatedSchema == null) {
            val builder: SchemaBuilder = if (sourceSchema == null) {
                SchemaUtil.copySchemaBasics(SchemaBuilder.struct())
            } else {
                SchemaUtil.copySchemaBasics(sourceSchema, SchemaBuilder.struct()).also { builder ->
                    // fix optional to true to prevent issues with is_deleted messages
                    // CREATE TABLE queries should be created with other fields as NULL except for record key and is_deleted as false
                    buildUpdatedSchema(sourceSchema, "", builder, true)
                }
            }
            builder.field("topic_key", convertFieldSchema(SchemaBuilder.string().build(), false, ""))
            builder.field("is_deleted", convertFieldSchema(SchemaBuilder.int8().build(), false, 0.toByte()))
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
            updatedValue.put("is_deleted", 0.toByte())
            buildWithSchema(sourceValue, "", updatedValue)
        } else if (sourceSchema != null) {
            // Tombstone case: sourceValue is null, populate defaults for non-optional fields
            populateDefaultsForNonOptionalFields(sourceSchema, updatedValue, "")
        }

        val bodyStruct = sourceValue?.getStruct("body")
        val deletedAt = bodyStruct?.get("deleted_at")

        // Handle delete events: body is null or deleted_at is set
        if (sourceValue == null || bodyStruct == null || deletedAt != null) {
            updatedValue.put("is_deleted", 1.toByte())
        }

        // Body is null case: populate defaults for non-optional body fields
        if (sourceValue != null && bodyStruct == null && sourceSchema != null) {
            val bodyField = sourceSchema.field("body")
            if (bodyField != null && bodyField.schema().type() == Schema.Type.STRUCT) {
                populateDefaultsForNonOptionalFields(bodyField.schema(), updatedValue, "body")
            }
        }

        return newRecord(record, updatedSchema, updatedValue)
    }
}
