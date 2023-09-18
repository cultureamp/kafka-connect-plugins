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

    private fun buildUpdatedSchema(schema: Schema, fieldNamePrefix: String, newSchema: SchemaBuilder, optional: Boolean) {
        for (field in schema.fields()) {
            val fieldName = fieldName(fieldNamePrefix, field.name())
            val fieldIsOptional = optional || field.schema().isOptional()
            val fieldDefaultValue = if (field.schema().defaultValue() != null) {
                field.schema().defaultValue() as Struct
            } else if (schema.defaultValue() != null) {
                val checkParent = schema.defaultValue() as Struct
                checkParent.get(field)
            } else {
                null
            }
            when (field.schema().type()) {
                Schema.Type.INT8 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.INT16 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.INT32 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.INT64 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.FLOAT32 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.FLOAT64 -> newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.BOOLEAN -> newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.STRING -> newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.BYTES -> newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.ARRAY -> newSchema.field(fieldName, convertFieldSchema(SchemaBuilder.string().build(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.MAP -> newSchema.field(fieldName, convertFieldSchema(SchemaBuilder.string().build(), fieldIsOptional, fieldDefaultValue))
                Schema.Type.STRUCT -> buildUpdatedSchema(field.schema(), fieldName, newSchema, fieldIsOptional)
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
            realValue = "[]"
            realSchema = SchemaBuilder.string().build()
        }
        if (value == null && schema.type() == Schema.Type.MAP) {
            realValue = "{}"
            realSchema = SchemaBuilder.string().build()
        }
        val converted = jsonConverter.fromConnectData("", realSchema, realValue)
        var fieldString = objectMapper.readTree(converted).toString()
        return fieldString.replace("\"[", "[").replace("]\"", "]").replace("\"{", "{").replace("}\"", "}")
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
            when (field.schema().type()) {
                Schema.Type.INT8 -> newRecord.put(fieldName, record.get(field))
                Schema.Type.INT16 -> newRecord.put(fieldName, record.get(field))
                Schema.Type.INT32 -> newRecord.put(fieldName, record.get(field))
                Schema.Type.INT64 -> newRecord.put(fieldName, record.get(field))
                Schema.Type.FLOAT32 -> newRecord.put(fieldName, record.get(field))
                Schema.Type.FLOAT64 -> newRecord.put(fieldName, record.get(field))
                Schema.Type.BOOLEAN -> newRecord.put(fieldName, record.get(field))
                Schema.Type.STRING -> newRecord.put(fieldName, record.get(field))
                Schema.Type.BYTES -> newRecord.put(fieldName, record.get(field))
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
                buildUpdatedSchema(sourceSchema, "", builder, sourceSchema.isOptional())
            }

            if (record.keySchema() != null) {
                builder.field("topic_key", record.keySchema())
            }
            builder.field("tombstone", convertFieldSchema(SchemaBuilder.bool().build(), false, false))
            updatedSchema = builder.build()
            schemaUpdateCache.put(sourceSchema, updatedSchema)
        }
        val updatedValue = Struct(updatedSchema)
        if (record.keySchema() != null && record.key() != null) {
            updatedValue.put("topic_key", record.key())
        }
        if (sourceValue != null) {
            updatedValue.put("tombstone", false)
            buildWithSchema(sourceValue, "", updatedValue)
        } else {
            updatedValue.put("tombstone", true)
        }
        return newRecord(record, updatedSchema, updatedValue)
    }

    private val objectMapper = ObjectMapper()
    private val jsonConverter = JsonConverter()
}
