package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class UnquoteRecordKeyTest {

    private fun createSmt(): UnquoteRecordKey<SinkRecord> {
        val smt = UnquoteRecordKey<SinkRecord>()
        smt.configure(mutableMapOf<String, Any>())
        return smt
    }

    private val valueSchema: Schema = SchemaBuilder.struct()
        .field("name", Schema.OPTIONAL_STRING_SCHEMA)
        .build()

    private fun sinkRecord(
        key: Any?,
        value: Struct? = Struct(valueSchema).put("name", "test"),
        timestamp: Long = 12345L,
    ): SinkRecord {
        return SinkRecord(
            "test-topic",
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            key,
            valueSchema,
            value,
            0L,
            timestamp,
            TimestampType.CREATE_TIME,
        )
    }

    @Test
    fun `strips surrounding quotes from key`() {
        val smt = createSmt()
        val record = sinkRecord("\"37d8c6b0-941f-41bc-a50a-9559dd9cabf1\"")

        val result = smt.apply(record)

        assertEquals("37d8c6b0-941f-41bc-a50a-9559dd9cabf1", result.key())
        assertEquals("test-topic", result.topic())
        assertEquals(12345L, result.timestamp())
    }

    @Test
    fun `key without quotes passes through unchanged`() {
        val smt = createSmt()
        val record = sinkRecord("37d8c6b0-941f-41bc-a50a-9559dd9cabf1")

        val result = smt.apply(record)

        assertEquals("37d8c6b0-941f-41bc-a50a-9559dd9cabf1", result.key())
    }

    @Test
    fun `null key passes through unchanged`() {
        val smt = createSmt()
        val record = sinkRecord(null)

        val result = smt.apply(record)

        assertNull(result.key())
    }

    @Test
    fun `non-string key passes through unchanged`() {
        val smt = createSmt()
        val keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build()
        val key = Struct(keySchema).put("id", "abc-123")
        val record = SinkRecord(
            "test-topic", 0, keySchema, key, valueSchema,
            Struct(valueSchema).put("name", "test"), 0L, 12345L, TimestampType.CREATE_TIME,
        )

        val result = smt.apply(record)

        assertEquals(key, result.key())
    }

    @Test
    fun `key with only one quote is not stripped`() {
        val smt = createSmt()
        val record = sinkRecord("\"37d8c6b0-941f-41bc-a50a-9559dd9cabf1")

        val result = smt.apply(record)

        assertEquals("\"37d8c6b0-941f-41bc-a50a-9559dd9cabf1", result.key())
    }

    @Test
    fun `value is preserved after stripping key`() {
        val smt = createSmt()
        val value = Struct(valueSchema).put("name", "important-data")
        val record = sinkRecord("\"some-key\"", value)

        val result = smt.apply(record)

        assertEquals("some-key", result.key())
        assertNotNull(result.value())
        assertEquals("important-data", (result.value() as Struct).get("name"))
    }

    @Test
    fun `partition and topic are preserved`() {
        val smt = createSmt()
        val record = SinkRecord(
            "my-topic", 5, Schema.OPTIONAL_STRING_SCHEMA, "\"quoted-key\"",
            valueSchema, Struct(valueSchema).put("name", "test"), 0L, 99999L, TimestampType.CREATE_TIME,
        )

        val result = smt.apply(record)

        assertEquals("quoted-key", result.key())
        assertEquals("my-topic", result.topic())
        assertEquals(5, result.kafkaPartition())
        assertEquals(99999L, result.timestamp())
    }
}
