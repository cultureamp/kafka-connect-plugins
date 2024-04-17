package com.cultureamp.kafka.connect.plugins.transforms

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.slf4j.LoggerFactory
import java.util.Collections

/**
 * A generic custom transform for RedShift
 *
 * This transformer class manipulates fields in schemas by turning them from arrays into strings
 * which is necessary for this:
 * https://stackoverflow.com/questions/61360342/kafka-connect-flatten-transformation-of-a-postgres-record-with-array-field issue to be solved
 * as RedShift does not support array types and arrays must be converted into strings.
 * See https://docs.confluent.io/platform/current/connect/javadocs/javadoc/org/apache/kafka/connect/transforms/Transformation.html.
 *
 * @param R is ConnectRecord<R>.
 * @constructor Creates a RedShiftComplexDataTypeTransformer Transformation<R> for a given ConnectRecord<T>
 */
class RedShiftComplexDataTypeTransformer<R : ConnectRecord<R>> : Transformation<R> {
    private val logger = LoggerFactory.getLogger(this::class.java.canonicalName)
    private val purpose = "RedShift™ Flatten and Complex Data Types to String Transform"
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

    private fun convertFieldSchema(orig: Schema, optional: Boolean, defaultFromParent: Any?, complexType: Boolean = false): Schema {
        // Note that we don't use the schema translation cache here. It might save us a bit of effort, but we really
        // only care about caching top-level schema translations.
        val builder = SchemaUtil.copySchemaBasics(orig)
        if (optional)
            builder.optional()
        if (defaultFromParent != null && !complexType)
            builder.defaultValue(defaultFromParent)
        return builder.build()
    }

    private fun buildUpdatedSchema(schema: Schema, fieldNamePrefix: String, newSchema: SchemaBuilder, optional: Boolean) {
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
                Schema.Type.ARRAY -> newSchema.field(fieldName, convertFieldSchema(SchemaBuilder.string().build(), optional, fieldDefaultValue, true))
                Schema.Type.MAP -> newSchema.field(fieldName, convertFieldSchema(SchemaBuilder.string().build(), optional, fieldDefaultValue, true))
                Schema.Type.STRUCT -> buildUpdatedSchema(field.schema(), fieldName, newSchema, optional)
                else -> throw DataException(
                    "Flatten transformation does not support " + field.schema().type() +
                        " for record with schemas (for field " + fieldName + ")."
                )
            }
        }
    }

    private fun convertToString(schema: Schema, value: Any?): String {
        var realValue = value
        var realSchema = schema
        if (value == null && schema.type() == Schema.Type.ARRAY) {
            if (schema.defaultValue() != null)
                realValue = schema.defaultValue()
            else
                realValue = "[]"
            realSchema = SchemaBuilder.string().build()
        }
        if (value == null && schema.type() == Schema.Type.MAP) {
            if (schema.defaultValue() != null)
                realValue = schema.defaultValue()
            else
                realValue = "{}"
            realSchema = SchemaBuilder.string().build()
        }
        val converted = jsonConverter.fromConnectData("", realSchema, realValue)
        return objectMapper.writeValueAsString(objectMapper.readTree(converted))
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
                Schema.Type.ARRAY -> newRecord.put(fieldName, convertToString(field.schema(), record.get(field)))
                Schema.Type.MAP -> newRecord.put(fieldName, convertToString(field.schema(), record.get(field)))
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
                // fix optional to true to prevent issues with tombstone messages
                // CREATE TABLE queries should be created with other fields as NULL except for record key and tombstone as false
                buildUpdatedSchema(sourceSchema, "", builder, true)
            }
            builder.field("topic_key", convertFieldSchema(SchemaBuilder.string().build(), false, ""))
            builder.field("tombstone", convertFieldSchema(SchemaBuilder.bool().build(), false, false))
            updatedSchema = builder.build()
            schemaUpdateCache.put(sourceSchema, updatedSchema)
        }
        val updatedValue = Struct(updatedSchema)
        if (record.key() != null) {
            updatedValue.put("topic_key", record.key().toString())
        }
        if (sourceValue != null) {
            updatedValue.put("tombstone", false)
            buildWithSchema(sourceValue, "", updatedValue)
        }
        if (sourceValue == null || sourceValue.get("body") == null) {
            updatedValue.put("tombstone", true)
        }
        return newRecord(record, updatedSchema, updatedValue)
    }

    private val objectMapper = ObjectMapper()
    private val jsonConverter = JsonConverter()
}
