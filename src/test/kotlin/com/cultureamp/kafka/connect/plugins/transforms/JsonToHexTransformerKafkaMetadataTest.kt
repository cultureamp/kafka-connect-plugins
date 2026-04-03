package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.Before
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Test for JsonToHexTransformer with Kafka metadata fields (always enabled)
 */
class JsonToHexTransformerKafkaMetadataTest {

    private lateinit var transformer: JsonToHexTransformer<SinkRecord>

    @Before
    fun setUp() {
        transformer = JsonToHexTransformer()
        transformer.configure(
            mutableMapOf<String, Any>(
                "hex.field.name" to "payload_hex"
            )
        )
    }

    @Test
    fun `should always include Kafka metadata fields`() {
        val testJson = mapOf("test" to "data", "nested" to mapOf("field" to "value"))

        val record = SinkRecord(
            "test-topic",
            2,
            null,
            null,
            null,
            testJson,
            156L,
            1713922160000L,
            TimestampType.CREATE_TIME
        )

        val result = transformer.apply(record)

        // Verify schema has all expected fields
        assertNotNull(result.valueSchema())
        assertEquals("HexEncodedJson", result.valueSchema().name())
        assertEquals(4, result.valueSchema().fields().size) // payload_hex + 3 metadata fields

        val resultStruct = result.value() as org.apache.kafka.connect.data.Struct

        // Verify hex payload is present
        assertNotNull(resultStruct.get("payload_hex"))

        // Verify Kafka metadata fields are present
        assertEquals(2, resultStruct.get("_kafka_metadata_partition"))
        assertEquals(156L, resultStruct.get("_kafka_metadata_offset"))
        assertEquals(1713922160000L, resultStruct.get("_kafka_metadata_timestamp"))

        println("✅ Transformer always includes all expected fields:")
        println("   - payload_hex: ${(resultStruct.get("payload_hex") as String).take(20)}...")
        println("   - _kafka_metadata_partition: ${resultStruct.get("_kafka_metadata_partition")}")
        println("   - _kafka_metadata_offset: ${resultStruct.get("_kafka_metadata_offset")}")
        println("   - _kafka_metadata_timestamp: ${resultStruct.get("_kafka_metadata_timestamp")}")
    }

    @Test
    fun `should handle OpenTelemetry data with Kafka metadata`() {
        val otelData = mapOf(
            "resourceSpans" to listOf(
                mapOf(
                    "resource" to mapOf(
                        "attributes" to listOf(
                            mapOf("key" to "service.name", "value" to mapOf("stringValue" to "ai-coach-api"))
                        )
                    ),
                    "scopeSpans" to listOf(
                        mapOf(
                            "scope" to mapOf("name" to "openinference.instrumentation.agno"),
                            "spans" to listOf(
                                mapOf(
                                    "traceId" to "0f60f165cd4491691af2d5bc36f1037b",
                                    "spanId" to "23ae78014c176545",
                                    "name" to "search_knowledge_base",
                                    "attributes" to listOf(
                                        mapOf("key" to "session_id", "value" to mapOf("stringValue" to "test-session"))
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )

        val record = SinkRecord(
            "opentelemetry-spans",
            0,
            null,
            null,
            null,
            otelData,
            999L,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME
        )

        val result = transformer.apply(record)
        val resultStruct = result.value() as org.apache.kafka.connect.data.Struct

        // Verify all fields are present
        assertNotNull(resultStruct.get("payload_hex"))
        assertEquals(0, resultStruct.get("_kafka_metadata_partition"))
        assertEquals(999L, resultStruct.get("_kafka_metadata_offset"))
        assertNotNull(resultStruct.get("_kafka_metadata_timestamp"))

        // Verify hex contains the OpenTelemetry data
        val hexValue = resultStruct.get("payload_hex") as String
        val originalBytes = hexValue.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
        val reconstructedJson = String(originalBytes, Charsets.UTF_8)

        assert(reconstructedJson.contains("ai-coach-api"))
        assert(reconstructedJson.contains("search_knowledge_base"))
        assert(reconstructedJson.contains("0f60f165cd4491691af2d5bc36f1037b"))

        println("✅ OpenTelemetry data with metadata processed successfully!")
        println("   - Hex length: ${hexValue.length}")
        println("   - Partition: ${resultStruct.get("_kafka_metadata_partition")}")
        println("   - Offset: ${resultStruct.get("_kafka_metadata_offset")}")
    }
}
