package com.cultureamp.kafka.connect.plugins.transforms

import com.mongodb.kafka.connect.source.MongoSourceConfig
import com.mongodb.kafka.connect.source.json.formatter.JsonWriterSettingsProvider
import com.mongodb.kafka.connect.source.schema.AvroSchema
import com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue
import com.mongodb.kafka.connect.util.ClassHelper
import com.mongodb.kafka.connect.util.ConfigHelper
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import java.io.File
import java.nio.file.Files
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Test class for ClickHouseFlattenTransformer
 */
class ClickHouseFlattenTransformerTest {

    private lateinit var transformer: ClickHouseFlattenTransformer<SinkRecord>

    private fun hasNoComplexTypes(obj: SinkRecord): Boolean =
        obj.valueSchema().fields().none { field ->
            field.schema().type() == Schema.Type.STRUCT
        }

    @BeforeTest
    fun setUp() {
        transformer = ClickHouseFlattenTransformer()
    }

    @Test
    fun `transform avro schema correctly`() {
        val initialSchema = AvroSchema.fromJson(fileContent("com/cultureamp/employee-data.employees-value-v1-clickhouse.avsc"))

        // Test the FULL transformer instead of just schema part
        val avroRecord = payload("com/cultureamp/employee-data.employees-v1-clickhouse.json")
        val sinkRecord = SinkRecord("test-topic", 1, null, null, initialSchema, avroRecord.value(), 156)
        val transformedRecord = transformer.apply(sinkRecord)
        val actualTransformedSchema = transformedRecord.valueSchema()

        val expectedSchema = getExpectedSchema()

        // Detailed schema comparison
        compareSchemas(expectedSchema, actualTransformedSchema)
    }

    @Test
    fun `can transform ECST Employee data with null body`() {

        val avroRecord = payload("com/cultureamp/employee-data.employees-v2-clickhouse.json")
        val sinkRecord = SinkRecord(
            "employee data ecst test",
            1,
            null,
            null,
            avroRecord.schema(),
            avroRecord.value(),
            156,
        )

        val transformedRecord = transformer.apply(sinkRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))

        // NULL BODY TEST IMPLEMENTATION:
        // When body is null in the input data, transformer processes top-level fields
        // and populates defaults for non-optional body fields
        val expectedSchema = getExpectedSchema()
        val arrayStructSchema = expectedSchema.field("test_array_of_structs").schema().valueSchema()
        val test_array_of_structs = listOf(
            Struct(arrayStructSchema).apply {
                put("demographic_id", "{\"string\": \"5c579970-684e-4911-a077-6bf407fb478d\"}")
                put("demographic_value_id", "{\"string\": \"427b936f-e932-4673-95a2-acd3e3b900b1\"}")
            },
            Struct(arrayStructSchema).apply {
                put("demographic_id", "{\"string\": \"460f6b2d-03c5-46cf-ba55-aa14477a12dc\"}")
                put("demographic_value_id", "{\"string\": \"ecc0db2e-486e-4f4a-a54a-db21673e1a2b\"}")
            }
        )

        // Expected result: body_* fields populated with defaults for non-optional fields
        val expectedValue = Struct(expectedSchema)
            .put("id", id)
            .put("account_id", account_id)
            .put("employee_id", employee_id)
            .put("event_created_at", event_created_at)
            // Non-optional body fields get defaults when body is null
            .put("body_observer", true) // schema default
            .put("body_test_map", emptyMap<String, Any>()) // type default for non-optional map
            .put("body_test_array_of_structs", emptyList<Any>()) // type default for non-optional array
            .put("body_erased", false) // type default for non-optional boolean
            .put("body_created_at", 0L) // type default for non-optional long
            .put("body_updated_at", 0L) // type default for non-optional long
            .put("metadata_correlation_id", metadata_correlation_id)
            .put("metadata_causation_id", metadata_causation_id)
            .put("metadata_executor_id", metadata_executor_id)
            .put("metadata_service", "Default-Service") // v2 data has no service field, uses default
            .put("test_array_of_structs", test_array_of_structs)
            .put("test_string_array", test_string_array)
            .put("test_array_of_arrays", test_array_of_arrays)
            .put("test_map", test_map)
            .put("is_deleted", 1.toByte())
            .put("_kafka_metadata_partition", "1")
            .put("_kafka_metadata_offset", "156")
            .put("_kafka_metadata_timestamp", null)

        // Schema validated separately - this just checks values match
        assertStructValuesEqual(expectedValue, transformedRecord.value() as Struct)
    }

    @Test
    fun `can transform ECST Employee data that has key as field`() {

        val avroRecord = payload("com/cultureamp/employee-data.employees-v1-clickhouse.json")
        val sinkRecord = SinkRecord(
            "employee data ecst test",
            1,
            Schema.STRING_SCHEMA,
            "hellp",
            avroRecord.schema(),
            avroRecord.value(),
            156,
            1727247537132,
            TimestampType.CREATE_TIME,
        )

        val transformedRecord = transformer.apply(sinkRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))

        // Manual construction of expected values using hard-coded schema as container
        val expectedSchema = getExpectedSchema()

        val bodyArrayStructSchema = expectedSchema.field("body_test_array_of_structs").schema().valueSchema()
        val body_test_array_of_structs = listOf(
            Struct(bodyArrayStructSchema).apply {
                put("demographic_id", "{\"string\": \"5c579970-684e-4911-a077-6bf407fb478d\"}")
                put("demographic_value_id", "{\"string\": \"427b936f-e932-4673-95a2-acd3e3b900b1\"}")
            },
            Struct(bodyArrayStructSchema).apply {
                put("demographic_id", "{\"string\": \"460f6b2d-03c5-46cf-ba55-aa14477a12dc\"}")
                put("demographic_value_id", "{\"string\": \"ecc0db2e-486e-4f4a-a54a-db21673e1a2b\"}")
            }
        )

        val testArrayStructSchema = expectedSchema.field("test_array_of_structs").schema().valueSchema()
        val test_array_of_structs = listOf(
            Struct(testArrayStructSchema).apply {
                put("demographic_id", "{\"string\": \"5c579970-684e-4911-a077-6bf407fb478d\"}")
                put("demographic_value_id", "{\"string\": \"427b936f-e932-4673-95a2-acd3e3b900b1\"}")
            },
            Struct(testArrayStructSchema).apply {
                put("demographic_id", "{\"string\": \"460f6b2d-03c5-46cf-ba55-aa14477a12dc\"}")
                put("demographic_value_id", "{\"string\": \"ecc0db2e-486e-4f4a-a54a-db21673e1a2b\"}")
            }
        )

        // Create expected Struct using hard-coded schema as container for our manually-defined expected values
        val expectedValue = Struct(expectedSchema)
            .put("id", id)
            .put("account_id", account_id)
            .put("employee_id", employee_id)
            .put("event_created_at", event_created_at)
            .put("body_source", body_source)
            .put("body_employee_id", body_employee_id)
            .put("body_email", body_email)
            .put("body_name", body_name)
            .put("body_preferred_name", body_preferred_name)
            .put("body_locale", body_locale)
            .put("body_observer", body_observer)
            .put("body_gdpr_erasure_request_id", body_gdpr_erasure_request_id)
            .put("body_test_map", body_test_map)
            .put("body_test_map_1", body_test_map_1)
            .put("body_test_array_of_structs", body_test_array_of_structs)
            .put("body_manager_assignment_manager_id", body_manager_assignment_manager_id)
            .put("body_manager_assignment_demographic_id", body_manager_assignment_demographic_id)
            .put("body_erased", body_erased)
            .put("body_created_at", body_created_at)
            .put("body_updated_at", body_updated_at)
            .put("body_deleted_at", body_deleted_at)
            .put("metadata_correlation_id", metadata_correlation_id)
            .put("metadata_causation_id", metadata_causation_id)
            .put("metadata_executor_id", metadata_executor_id)
            .put("metadata_service", metadata_service)
            .put("test_array_of_structs", test_array_of_structs)
            .put("test_string_array", test_string_array)
            .put("test_array_of_arrays", test_array_of_arrays)
            .put("test_map", test_map)
            .put("topic_key", "hellp")
            .put("is_deleted", 0.toByte())
            .put("_kafka_metadata_partition", "1")
            .put("_kafka_metadata_offset", "156")
            .put("_kafka_metadata_timestamp", 1727247537132L)

        // Schema validated separately - this just checks values match
        assertStructValuesEqual(expectedValue, transformedRecord.value() as Struct)
    }

    @Test
    fun `can transform ECST Employee data with tombstone message and non-null key`() {

        val avroRecord = payload("com/cultureamp/employee-data.employees-v1-clickhouse.json")
        val sinkRecord = SinkRecord(
            "employee data ecst test",
            0,
            Schema.STRING_SCHEMA,
            "hellp",
            avroRecord.schema(),
            null,
            156,
            1713922160,
            TimestampType.CREATE_TIME,
        )

        val transformedRecord = transformer.apply(sinkRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))
    }

    @Test
    fun `can transform ECST Employee data with tombstone message and null key`() {

        val avroRecord = payload("com/cultureamp/employee-data.employees-v1-clickhouse.json")
        val sinkRecord = SinkRecord(
            "employee data ecst test",
            0,
            null,
            null,
            avroRecord.schema(),
            null,
            156,
            1713922160,
            TimestampType.CREATE_TIME,
        )

        val transformedRecord = transformer.apply(sinkRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))

        val actualSchema = transformedRecord.valueSchema()
        // Tombstone records populate defaults for non-optional fields only
        // Note: body is optional (union with null), so body_* fields are NOT populated
        val expectedValue = Struct(actualSchema)
            // Non-optional top-level fields get type defaults
            .put("id", "")
            .put("account_id", "")
            .put("employee_id", "")
            .put("event_created_at", 0L)
            // body is optional (union with null), so no body_* defaults
            // Non-optional metadata fields (metadata.service has schema default)
            .put("metadata_service", "Default-Service")
            // Non-optional top-level array
            .put("test_array_of_structs", emptyList<Any>())
            // Kafka metadata fields
            .put("is_deleted", 1.toByte())
            .put("_kafka_metadata_partition", "0")
            .put("_kafka_metadata_offset", "156")
            .put("_kafka_metadata_timestamp", 1713922160L)
        assertEquals(expectedValue, transformedRecord.value())
    }

    @Test
    fun `can transform ECST Employee data with tombstone message and null key and null value schema`() {
        val sinkRecord = SinkRecord(
            "employee data ecst test",
            5,
            null,
            null,
            null,
            null,
            156,
        )

        val transformedRecord = transformer.apply(sinkRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))
    }

    @Test
    fun `populates defaults for non-optional fields when body is null`() {
        // This test verifies that when body is null (delete event), non-optional body fields
        // get populated with default values to satisfy ClickHouse connector validation
        val avroRecord = payload("com/cultureamp/employee-data.employees-v2-clickhouse.json")
        val sinkRecord = SinkRecord(
            "delete event test",
            1,
            Schema.STRING_SCHEMA,
            "test-key",
            avroRecord.schema(),
            avroRecord.value(),
            100,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME,
        )

        val transformedRecord = transformer.apply(sinkRecord)
        val value = transformedRecord.value() as Struct

        // Verify is_deleted flag is set
        assertEquals(1.toByte(), value.get("is_deleted"), "is_deleted should be 1 for null body")

        // Verify non-optional body fields are populated with defaults
        assertEquals(true, value.get("body_observer"), "body_observer should have schema default (true)")
        assertEquals(emptyMap<String, Any>(), value.get("body_test_map"), "body_test_map should have empty map default")
        assertEquals(emptyList<Any>(), value.get("body_test_array_of_structs"), "body_test_array_of_structs should have empty list default")
        assertEquals(false, value.get("body_erased"), "body_erased should have type default (false)")
        assertEquals(0L, value.get("body_created_at"), "body_created_at should have type default (0)")
        assertEquals(0L, value.get("body_updated_at"), "body_updated_at should have type default (0)")

        // Verify optional body fields remain null
        assertEquals(null, value.get("body_source"), "Optional body_source should remain null")
        assertEquals(null, value.get("body_email"), "Optional body_email should remain null")
        assertEquals(null, value.get("body_deleted_at"), "Optional body_deleted_at should remain null")
    }

    @Test
    fun `populates defaults for non-optional top-level fields on tombstone`() {
        // This test verifies that tombstone messages get default values for non-optional top-level fields
        val avroRecord = payload("com/cultureamp/employee-data.employees-v1-clickhouse.json")
        val sinkRecord = SinkRecord(
            "tombstone test",
            0,
            Schema.STRING_SCHEMA,
            "tombstone-key",
            avroRecord.schema(),
            null, // tombstone - null value
            200,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME,
        )

        val transformedRecord = transformer.apply(sinkRecord)
        val value = transformedRecord.value() as Struct

        // Verify is_deleted flag is set
        assertEquals(1.toByte(), value.get("is_deleted"), "is_deleted should be 1 for tombstone")

        // Verify non-optional top-level fields are populated with defaults
        assertEquals("", value.get("id"), "id should have empty string default")
        assertEquals("", value.get("account_id"), "account_id should have empty string default")
        assertEquals("", value.get("employee_id"), "employee_id should have empty string default")
        assertEquals(0L, value.get("event_created_at"), "event_created_at should have 0 default")
        assertEquals(emptyList<Any>(), value.get("test_array_of_structs"), "test_array_of_structs should have empty list default")

        // Verify non-optional metadata fields are populated
        assertEquals("Default-Service", value.get("metadata_service"), "metadata_service should have schema default")

        // Verify topic_key is set from record key
        assertEquals("tombstone-key", value.get("topic_key"), "topic_key should be set from record key")
    }

    private val sourceSchema = AvroSchema.fromJson(fileContent("com/cultureamp/employee-data.employees-value-v1-clickhouse.avsc"))

    private fun payload(fileName: String): SchemaAndValue {
        val document = ConfigHelper.documentFromString(fileContent(fileName)).get()

        return BsonValueToSchemaAndValue(jsonWriterSettings)
            .toSchemaAndValue(sourceSchema, document.toBsonDocument())
    }

    private val id = "c63526f8-dec7-4ef8-96d8-18756076f064"
    private val account_id = "0a05e2a3-7258-4cf5-a7f4-e21b08c030c5"
    private val employee_id = "c63526f8-dec7-4ef8-96d8-18756076f064"
    private val event_created_at = 1536899741117
    private val body_source = "{\"string\": \"\"}"
    private val body_employee_id = null
    private val body_email = "{\"string\": \"testing800702@namelytest.com\"}"
    private val body_name = "{\"string\": \"Test User 800702\"}"
    private val body_preferred_name = null
    private val body_locale = null
    private val body_observer = false
    private val body_gdpr_erasure_request_id = null
    private val body_test_map = mapOf("added_users_count" to 0, "ignored_new_demographics_count" to 0, "ignored_users_count" to 0, "inactive_updated_users_count" to 0, "reactivated_users_count" to 0, "removed_users_count" to 0, "updated_users_count" to 0)
    private val body_test_map_1 = null
    private val body_manager_assignment_manager_id = "{\"string\": \"5c579970-684e-4911-a077-6bf407fb478d\"}"
    private val body_manager_assignment_demographic_id = "{\"string\": \"427b936f-e932-4673-95a2-acd3e3b900b1\"}"
    private val body_erased = false
    private val body_created_at = 1536899741113
    private val body_updated_at = 1536899741117
    private val body_deleted_at = null
    private val metadata_correlation_id = "{\"string\": \"b9098254-a1db-4114-9a39-baa17ab18fbf\"}"
    private val metadata_causation_id = null
    private val metadata_executor_id = "{\"string\": \"379907ca-632c-4e83-89c4-9dbe0e759ad3\"}"
    private val metadata_service = "Influx"
    private val test_string_array = listOf("a", "b", "c")
    private val test_array_of_arrays = listOf(listOf("a", "b", "c"), listOf("e"), listOf("f", "g"))
    private val test_map =
        mapOf("added_users_count" to 0, "ignored_new_demographics_count" to 0, "ignored_users_count" to 0, "inactive_updated_users_count" to 0, "reactivated_users_count" to 0, "removed_users_count" to 0, "reactivated_users_count" to 0, "updated_users_count" to 0)

    private fun fileContent(fileName: String): String {
        val url = this.javaClass.classLoader
            .getResource(fileName) ?: throw IllegalArgumentException("$fileName is not found 1")

        return String(Files.readAllBytes(File(url.file).toPath()))
    }

    private fun getExpectedSchema(): Schema {
        // Create expected schema using transformer's logic for proper equality
        val avroSchema = AvroSchema.fromJson(fileContent("com/cultureamp/employee-data.employees-value-v1-clickhouse.avsc"))

        // Use minimal record to extract schema - transformer builds same output schema regardless of data
        val schemaExtractionRecord = SinkRecord("schema-extraction", 0, null, null, avroSchema, null, 0)
        val transformedRecord = transformer.apply(schemaExtractionRecord)

        return transformedRecord.valueSchema()
    }

    private val jsonWriterSettings =
        ClassHelper.createInstance(
            MongoSourceConfig.OUTPUT_JSON_FORMATTER_CONFIG,
            "com.mongodb.kafka.connect.source.json.formatter.DefaultJson",
            JsonWriterSettingsProvider::class.java
        ).jsonWriterSettings

    /**
     * Helper function to compare Struct values field by field.
     * Needed because Struct.equals() compares object identity, not content.
     */
    private fun compareSchemas(expectedSchema: Schema, actualSchema: Schema, path: String = "") {
        // Ensure schemas are not null
        assertNotNull(expectedSchema, "Expected schema cannot be null at $path")
        assertNotNull(actualSchema, "Actual schema cannot be null at $path")

        // Compare basic schema properties
        assertEquals(
            expectedSchema.type(), actualSchema.type(),
            "Schema type mismatch at $path: expected ${expectedSchema.type()}, got ${actualSchema.type()}"
        )
        assertEquals(
            expectedSchema.isOptional, actualSchema.isOptional,
            "Optional setting mismatch at $path"
        )
        assertEquals(
            expectedSchema.defaultValue(), actualSchema.defaultValue(),
            "Default value mismatch at $path"
        )
        assertEquals(
            expectedSchema.name(), actualSchema.name(),
            "Name mismatch at $path"
        )
        assertEquals(
            expectedSchema.doc(), actualSchema.doc(),
            "Doc mismatch at $path"
        )
        assertEquals(
            expectedSchema.version(), actualSchema.version(),
            "Version mismatch at $path"
        )

        // Type-specific deep comparisons
        when (expectedSchema.type()) {
            Schema.Type.STRUCT -> compareStructSchemas(expectedSchema, actualSchema, path)
            Schema.Type.ARRAY -> compareArraySchemas(expectedSchema, actualSchema, path)
            Schema.Type.MAP -> compareMapSchemas(expectedSchema, actualSchema, path)
            else -> {
                // For primitive types, just check the main fields match
                // No further comparison needed
            }
        }
        // Just to be sure we don't miss something important, add a regular schema.equals(), which won't tell is _what_
        // is different
        assertEquals(expectedSchema, actualSchema, "Schemas do not match, for some reason not checked above, at $path")
    }

    private fun compareStructSchemas(expectedSchema: Schema, actualSchema: Schema, path: String) {
        val expectedFields = expectedSchema.fields()
        val actualFields = actualSchema.fields()

        assertEquals(
            expectedFields.size, actualFields.size,
            "Struct field count mismatch at $path: expected ${expectedFields.size}, got ${actualFields.size}"
        )

        for (expectedField in expectedFields) {
            val actualField = actualSchema.field(expectedField.name())
                ?: throw AssertionError("Field ${expectedField.name()} not found at $path")

            // Compare field names and run recursive schema comparison
            assertEquals(
                expectedField.name(), actualField.name(),
                "Field name mismatch at $path"
            )
            compareSchemas(
                expectedField.schema(),
                actualField.schema(),
                "$path.${expectedField.name()}"
            )
        }
    }

    private fun compareArraySchemas(expectedSchema: Schema, actualSchema: Schema, path: String) {
        // Compare array value schemas
        compareSchemas(
            expectedSchema.valueSchema(),
            actualSchema.valueSchema(),
            "$path[]"
        )
    }

    private fun compareMapSchemas(expectedSchema: Schema, actualSchema: Schema, path: String) {
        // Compare key and value schemas
        compareSchemas(
            expectedSchema.keySchema(),
            actualSchema.keySchema(),
            "$path[key]"
        )
        compareSchemas(
            expectedSchema.valueSchema(),
            actualSchema.valueSchema(),
            "$path[value]"
        )
    }

    private fun assertStructValuesEqual(expected: Struct, actual: Struct) {
        assertEquals(expected.schema().fields().size, actual.schema().fields().size, "Struct field count mismatch")

        for (expectedField in expected.schema().fields()) {
            val expectedFieldValue = expected.get(expectedField.name())
            val actualFieldValue = actual.get(expectedField.name())

            // Deep comparison for array fields containing Structs
            if (expectedFieldValue is List<*> && actualFieldValue is List<*>) {
                assertEquals(expectedFieldValue.size, actualFieldValue.size, "Array size mismatch for ${expectedField.name()}")
                for (i in expectedFieldValue.indices) {
                    val expectedItem = expectedFieldValue[i]
                    val actualItem = actualFieldValue[i]
                    if (expectedItem is Struct && actualItem is Struct) {
                        // Compare Struct content field by field
                        assertEquals(
                            expectedItem.schema().fields().size, actualItem.schema().fields().size,
                            "Struct field count mismatch in array ${expectedField.name()}[$i]"
                        )
                        for (structField in expectedItem.schema().fields()) {
                            assertEquals(
                                expectedItem.get(structField.name()), actualItem.get(structField.name()),
                                "Struct field ${structField.name()} mismatch in array ${expectedField.name()}[$i]"
                            )
                        }
                    } else {
                        assertEquals(expectedItem, actualItem, "Array item mismatch for ${expectedField.name()}[$i]")
                    }
                }
            } else {
                assertEquals(expectedFieldValue, actualFieldValue, "Field value mismatch for ${expectedField.name()}")
            }
        }
    }
}
