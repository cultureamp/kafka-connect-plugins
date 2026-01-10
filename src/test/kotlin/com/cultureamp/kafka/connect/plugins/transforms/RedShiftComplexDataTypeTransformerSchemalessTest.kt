package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.Before
import kotlin.test.Test
import kotlin.test.assertEquals

class RedShiftComplexDataTypeTransformerSchemalessTest {
    private lateinit var transformer: RedShiftComplexDataTypeTransformer<SinkRecord>

    @Before
    fun setUp() {
        transformer = RedShiftComplexDataTypeTransformer()
    }

    @Test
    fun `converts schemaless map value to struct with json_payload`() {
        val mapValue = mapOf(
            "foo" to 123,
            "bar" to listOf(1, 2, 3),
            "baz" to mapOf("nested" to true)
        )
        val record = SinkRecord(
            "test-topic",
            0,
            null,
            null,
            null,
            mapValue,
            42L,
            1000L, // timestamp
            TimestampType.CREATE_TIME
        )
        val transformed = transformer.apply(record)
        val expectedSchema = SchemaBuilder.struct()
            .field("json_payload", Schema.STRING_SCHEMA)
            .field("topic_key", Schema.OPTIONAL_STRING_SCHEMA)
            .field("tombstone", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("_kafka_metadata_partition", Schema.OPTIONAL_STRING_SCHEMA)
            .field("_kafka_metadata_offset", Schema.OPTIONAL_STRING_SCHEMA)
            .field("_kafka_metadata_timestamp", Schema.OPTIONAL_STRING_SCHEMA)
            .build()

        // Verify the transformed record has the expected structure
        assertEquals(expectedSchema, transformed.valueSchema())
        val transformedValue = transformed.value() as Struct
        assertEquals(false, transformedValue.get("tombstone"))
        assertEquals("0", transformedValue.get("_kafka_metadata_partition"))
        assertEquals("42", transformedValue.get("_kafka_metadata_offset"))
        assertEquals("1000", transformedValue.get("_kafka_metadata_timestamp"))
        assertEquals(null, transformedValue.get("topic_key"))
        // Verify json_payload contains the map data as JSON string
        val jsonPayload = transformedValue.get("json_payload") as String
        assertEquals(true, jsonPayload.contains("\"foo\":123"))
        assertEquals(true, jsonPayload.contains("\"bar\":[1,2,3]"))
        assertEquals(true, jsonPayload.contains("\"baz\":{\"nested\":true}"))
    }
}
