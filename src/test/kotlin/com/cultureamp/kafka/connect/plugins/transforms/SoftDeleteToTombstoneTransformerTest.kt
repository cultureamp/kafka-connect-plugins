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

class SoftDeleteToTombstoneTransformerTest {

    private fun configure(vararg props: Pair<String, Any>): SoftDeleteToTombstoneTransformer<SinkRecord> {
        val transformProperties = mutableMapOf<String, Any>()
        for (prop in props) {
            transformProperties[prop.first] = prop.second
        }
        val smt = SoftDeleteToTombstoneTransformer<SinkRecord>()
        smt.configure(transformProperties)
        return smt
    }

    private val bodySchema: Schema = SchemaBuilder.struct()
        .field("deleted_at", Schema.OPTIONAL_INT64_SCHEMA)
        .field("name", Schema.OPTIONAL_STRING_SCHEMA)
        .build()

    private val valueSchema: Schema = SchemaBuilder.struct()
        .field("body", bodySchema)
        .field("metadata", Schema.OPTIONAL_STRING_SCHEMA)
        .build()

    private val keySchema: Schema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .build()

    private fun sinkRecord(
        value: Struct?,
        key: Struct? = Struct(keySchema).put("id", "abc-123"),
        timestamp: Long = 12345L,
    ): SinkRecord {
        return SinkRecord(
            "test-topic",
            0,
            keySchema,
            key,
            valueSchema,
            value,
            0L,
            timestamp,
            TimestampType.CREATE_TIME,
        )
    }

    @Test
    fun `soft deleted record is converted to tombstone`() {
        val smt = configure()
        val body = Struct(bodySchema).put("deleted_at", 1700000000000L).put("name", "Test")
        val value = Struct(valueSchema).put("body", body).put("metadata", "some-meta")
        val key = Struct(keySchema).put("id", "abc-123")
        val record = sinkRecord(value, key)

        val result = smt.apply(record)

        assertNull(result.value(), "Tombstone record should have null value")
        assertNull(result.valueSchema(), "Tombstone record should have null value schema")
        assertNotNull(result.key(), "Key must be preserved for JDBC sink DELETE")
        assertEquals("abc-123", (result.key() as Struct).get("id"))
        assertEquals("test-topic", result.topic())
        assertEquals(12345L, result.timestamp())
    }

    @Test
    fun `normal record passes through unchanged`() {
        val smt = configure()
        val body = Struct(bodySchema).put("deleted_at", null).put("name", "Test")
        val value = Struct(valueSchema).put("body", body).put("metadata", "some-meta")
        val record = sinkRecord(value)

        val result = smt.apply(record)

        assertNotNull(result.value(), "Non-deleted record should pass through")
        assertEquals(value, result.value())
    }

    @Test
    fun `existing tombstone passes through unchanged`() {
        val smt = configure()
        val record = sinkRecord(null)

        val result = smt.apply(record)

        assertNull(result.value(), "Tombstone should remain a tombstone")
    }

    @Test
    fun `custom field path works correctly`() {
        val metadataSchema = SchemaBuilder.struct()
            .field("removed_at", Schema.OPTIONAL_STRING_SCHEMA)
            .build()
        val customSchema = SchemaBuilder.struct()
            .field("metadata", metadataSchema)
            .build()

        val smt = configure("field" to "metadata.removed_at")
        val metadata = Struct(metadataSchema).put("removed_at", "2024-01-01T00:00:00Z")
        val value = Struct(customSchema).put("metadata", metadata)
        val key = Struct(keySchema).put("id", "def-456")
        val record = SinkRecord("custom-topic", 0, keySchema, key, customSchema, value, 0L, 99L, TimestampType.CREATE_TIME)

        val result = smt.apply(record)

        assertNull(result.value(), "Record with custom soft-delete field set should become tombstone")
        assertEquals("def-456", (result.key() as Struct).get("id"))
    }

    @Test
    fun `single level field path works`() {
        val flatSchema = SchemaBuilder.struct()
            .field("deleted_at", Schema.OPTIONAL_INT64_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build()

        val smt = configure("field" to "deleted_at")
        val value = Struct(flatSchema).put("deleted_at", 1700000000000L).put("name", "Test")
        val key = Struct(keySchema).put("id", "ghi-789")
        val record = SinkRecord("flat-topic", 0, keySchema, key, flatSchema, value, 0L, 50L, TimestampType.CREATE_TIME)

        val result = smt.apply(record)

        assertNull(result.value(), "Record with top-level deleted_at set should become tombstone")
    }

    @Test
    fun `null intermediate struct passes through unchanged`() {
        val outerSchema = SchemaBuilder.struct()
            .field("body", SchemaBuilder.struct().field("deleted_at", Schema.OPTIONAL_INT64_SCHEMA).optional().build())
            .build()

        val smt = configure()
        val value = Struct(outerSchema).put("body", null)
        val record = SinkRecord("test-topic", 0, keySchema, Struct(keySchema).put("id", "abc"), outerSchema, value, 0L, 100L, TimestampType.CREATE_TIME)

        val result = smt.apply(record)

        assertNotNull(result.value(), "Record with null intermediate struct should pass through")
    }

    @Test
    fun `key and topic are preserved in tombstone`() {
        val smt = configure()
        val body = Struct(bodySchema).put("deleted_at", 1700000000000L)
        val value = Struct(valueSchema).put("body", body)
        val key = Struct(keySchema).put("id", "preserve-me")
        val record = SinkRecord("important-topic", 3, keySchema, key, valueSchema, value, 0L, 99999L, TimestampType.CREATE_TIME)

        val result = smt.apply(record)

        assertNull(result.value())
        assertEquals("important-topic", result.topic())
        assertEquals(3, result.kafkaPartition())
        assertEquals(keySchema, result.keySchema())
        assertEquals("preserve-me", (result.key() as Struct).get("id"))
        assertEquals(99999L, result.timestamp())
    }
}
