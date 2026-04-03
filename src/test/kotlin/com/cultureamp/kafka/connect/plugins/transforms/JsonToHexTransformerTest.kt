package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.Before
import java.nio.charset.StandardCharsets
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Unit tests for JsonToHexTransformer
 */
class JsonToHexTransformerTest {

    private lateinit var transformer: JsonToHexTransformer<SinkRecord>

    @Before
    fun setUp() {
        transformer = JsonToHexTransformer()
        transformer.configure(mutableMapOf<String, Any>())
    }

    @Test
    fun `should convert simple JSON string to hex`() {
        val jsonString = """{"name":"John","age":30}"""
        val expectedHex = stringToHex(jsonString)

        val record = SinkRecord(
            "test-topic",
            0,
            null,
            null,
            null,
            jsonString,
            0L,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME
        )

        val result = transformer.apply(record)

        assertNotNull(result.valueSchema())
        assertEquals("HexEncodedJson", result.valueSchema().name())
        assertEquals(Schema.Type.STRUCT, result.valueSchema().type())

        val resultStruct = result.value() as org.apache.kafka.connect.data.Struct
        assertEquals(expectedHex, resultStruct.get("json_hex"))
    }

    @Test
    fun `should convert nested JSON object to hex`() {
        val nestedJson = mapOf(
            "user" to mapOf(
                "name" to "John Doe",
                "details" to mapOf(
                    "age" to 30,
                    "city" to "New York",
                    "preferences" to listOf("coding", "reading")
                )
            ),
            "metadata" to mapOf(
                "timestamp" to "2023-01-01T00:00:00Z",
                "version" to 1
            )
        )

        val record = SinkRecord(
            "test-topic",
            0,
            null,
            null,
            null,
            nestedJson,
            0L,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME
        )

        val result = transformer.apply(record)

        assertNotNull(result.valueSchema())
        val resultStruct = result.value() as org.apache.kafka.connect.data.Struct
        val hexValue = resultStruct.get("json_hex") as String

        // Verify it's valid hex (even length, only hex chars)
        assertEquals(0, hexValue.length % 2)
        assert(hexValue.matches(Regex("[0-9a-f]+")))

        // Verify we can convert back to original JSON structure
        val originalBytes = hexValue.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
        val reconstructedJson = String(originalBytes, StandardCharsets.UTF_8)
        assert(reconstructedJson.contains("John Doe"))
        assert(reconstructedJson.contains("New York"))
        assert(reconstructedJson.contains("coding"))
    }

    @Test
    fun `should handle empty JSON object`() {
        val emptyJson = "{}"
        val expectedHex = stringToHex(emptyJson)

        val record = SinkRecord(
            "test-topic",
            0,
            null,
            null,
            null,
            emptyJson,
            0L,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME
        )

        val result = transformer.apply(record)
        val resultStruct = result.value() as org.apache.kafka.connect.data.Struct
        assertEquals(expectedHex, resultStruct.get("json_hex"))
    }

    @Test
    fun `should handle JSON array`() {
        val jsonArray = listOf(
            mapOf("id" to 1, "name" to "Item 1"),
            mapOf("id" to 2, "name" to "Item 2")
        )

        val record = SinkRecord(
            "test-topic",
            0,
            null,
            null,
            null,
            jsonArray,
            0L,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME
        )

        val result = transformer.apply(record)
        val resultStruct = result.value() as org.apache.kafka.connect.data.Struct
        val hexValue = resultStruct.get("json_hex") as String

        // Verify it's valid hex
        assertEquals(0, hexValue.length % 2)
        assert(hexValue.matches(Regex("[0-9a-f]+")))
    }

    @Test
    fun `should handle large JSON payload`() {
        // Create a larger JSON object
        val largeData = mutableMapOf<String, Any>()
        repeat(1000) { i ->
            largeData["field_$i"] = "This is a longer string value for field $i with some additional content to make it bigger"
        }
        largeData["nested"] = mapOf(
            "level1" to mapOf(
                "level2" to mapOf(
                    "level3" to "deep nested value"
                )
            )
        )

        val record = SinkRecord(
            "test-topic",
            0,
            null,
            null,
            null,
            largeData,
            0L,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME
        )

        val result = transformer.apply(record)
        val resultStruct = result.value() as org.apache.kafka.connect.data.Struct
        val hexValue = resultStruct.get("json_hex") as String

        // Verify it's valid hex and reasonably large
        assertEquals(0, hexValue.length % 2)
        assert(hexValue.matches(Regex("[0-9a-f]+")))
        assert(hexValue.length > 1000) // Should be quite large
    }

    @Test
    fun `should use custom field name when configured`() {
        val customTransformer = JsonToHexTransformer<SinkRecord>()
        customTransformer.configure(mutableMapOf<String, Any>("hex.field.name" to "custom_hex_field"))

        val jsonString = """{"test":"value"}"""
        val expectedHex = stringToHex(jsonString)

        val record = SinkRecord(
            "test-topic",
            0,
            null,
            null,
            null,
            jsonString,
            0L,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME
        )

        val result = customTransformer.apply(record)
        val resultStruct = result.value() as org.apache.kafka.connect.data.Struct
        assertEquals(expectedHex, resultStruct.get("custom_hex_field"))
    }

    @Test
    fun `should handle special characters and unicode`() {
        val jsonWithSpecialChars = mapOf(
            "emoji" to "🚀",
            "special" to "Special chars: àáâãäåæçèéêë",
            "quotes" to "Text with \"quotes\" and 'apostrophes'",
            "newlines" to "Line 1\nLine 2\tTabbed"
        )

        val record = SinkRecord(
            "test-topic",
            0,
            null,
            null,
            null,
            jsonWithSpecialChars,
            0L,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME
        )

        val result = transformer.apply(record)
        val resultStruct = result.value() as org.apache.kafka.connect.data.Struct
        val hexValue = resultStruct.get("json_hex") as String

        // Verify it's valid hex
        assertEquals(0, hexValue.length % 2)
        assert(hexValue.matches(Regex("[0-9a-f]+")))

        // Verify we can reconstruct and it contains our special characters
        val originalBytes = hexValue.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
        val reconstructedJson = String(originalBytes, StandardCharsets.UTF_8)
        assert(reconstructedJson.contains("🚀"))
        assert(reconstructedJson.contains("àáâãäåæçèéêë"))
    }

    /**
     * Helper function to convert string to hex for testing
     */
    private fun stringToHex(input: String): String {
        return input.toByteArray(StandardCharsets.UTF_8)
            .joinToString("") { "%02x".format(it) }
    }
}
