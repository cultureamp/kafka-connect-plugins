package com.cultureamp.kafka.connect.plugins.transforms

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
            42L
        )
        val transformed = transformer.apply(record)
        val expectedSchema = SchemaBuilder.struct().field("json_payload", Schema.STRING_SCHEMA).build()
        val expectedJson = "{" +
            "\"foo\":123," +
            "\"bar\":[1,2,3]," +
            "\"baz\":{\"nested\":true}" +
            "}"
        val expectedStruct = Struct(expectedSchema).put("json_payload", expectedJson)
        assertEquals(expectedSchema, transformed.valueSchema())
        assertEquals(expectedStruct, transformed.value())
    }
}
