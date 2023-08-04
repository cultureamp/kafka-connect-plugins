package com.cultureamp.kafka.connect.plugins.transforms

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
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
 * @constructor Creates a RedShiftArrayTransformer Transformation<R> for a given ConnectRecord<T>
 */
class RedShiftArrayTransformer<R : ConnectRecord<R>> : Transformation<R> {
    private val logger = LoggerFactory.getLogger(this::class.java.canonicalName)
    private val purpose = "RedShift™ JSON Array to String Transform"

    override fun configure(configs: MutableMap<String, *>?) {}

    override fun config(): ConfigDef {
        return ConfigDef()
    }

    override fun close() {}

    override fun apply(record: R): R {
        try {
            val sourceValue = Requirements.requireStruct(record.value(), purpose)
            val targetPayload = targetPayload(sourceValue, record.valueSchema())

            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                targetPayload.schema(),
                targetPayload,
                record.timestamp()
            )
        } catch (e: Exception) {
            logger.error("Exception: ", e)
            logger.error("Record Received: " + record.value())
            throw e
        }
    }

    private fun updateSchema(field: Field): Schema {
        if (field.schema().type() == Schema.Type.ARRAY) {
            return SchemaBuilder.string().build()
        }
        return field.schema()
    }

    private fun targetPayload(sourceValue: Struct, sourceSchema: Schema): Struct {
        val props = Collections.singletonMap("schemas.enable", false)
        jsonConverter.configure(props, true)
        val builder = SchemaUtil.copySchemaBasics(sourceSchema, SchemaBuilder.struct())
        for (field in sourceSchema.fields()) {
            builder.field(field.name(), updateSchema(field))
        }
        val newSchema = builder.build()
        val targetPayload = Struct(newSchema)
        for (field in newSchema.fields()) {
            val fieldVal = sourceValue.get(field.name())
            val fieldSchema = sourceSchema.field(field.name()).schema()
            if (field.schema().type() == fieldSchema.type()) {
                targetPayload.put(field.name(), fieldVal)
            } else {
                val converted = jsonConverter.fromConnectData("", fieldSchema, fieldVal)
                var fieldString = objectMapper.readTree(converted).toString()
                fieldString = fieldString.replace("\"[", "[").replace("]\"", "]").replace("\"{", "{").replace("}\"", "}")
                targetPayload.put(field.name(), fieldString)
            }
        }
        return targetPayload
    }

    private val objectMapper = ObjectMapper()
    private val jsonConverter = JsonConverter()
}
