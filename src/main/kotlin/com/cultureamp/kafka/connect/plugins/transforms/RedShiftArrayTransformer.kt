package com.cultureamp.kafka.connect.plugins.transforms

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.slf4j.LoggerFactory

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
                record.valueSchema(),
                targetPayload,
                record.timestamp()
            )
        } catch (e: Exception) {
            logger.error("Exception: ", e)
            logger.error("Record Received: " + record.value())
            throw e
        }
    }

    private fun arrayToStringMapping(obj: Any) {
        when (obj) {
            is Array<*> -> objectMapper.writeValueAsString(obj)
            else -> obj
        }
    }

    private fun targetPayload(sourceValue: Struct, targetSchema: Schema): Struct {

        targetSchema::class.members.forEach { member -> arrayToStringMapping(member) }

        val targetPayload = Struct(targetSchema)

        return targetPayload
    }

    private val objectMapper = ObjectMapper()
}
