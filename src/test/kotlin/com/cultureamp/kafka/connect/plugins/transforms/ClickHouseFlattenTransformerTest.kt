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
import org.junit.jupiter.api.BeforeEach
import java.io.File
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Test class for ClickHouseFlattenTransformer
 */
class ClickHouseFlattenTransformerTest {

    private lateinit var transformer: ClickHouseFlattenTransformer<SinkRecord>

    private fun hasNoComplexTypes(obj: SinkRecord): Boolean {

        var hasNoComplexTypes = true
        for (field in obj.valueSchema().fields()) {
            if (field.schema().type() == Schema.Type.STRUCT) {
                hasNoComplexTypes = false
            }
        }
        return hasNoComplexTypes
    }

    @BeforeEach
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

        // Schema objects don't implement proper equals() so we need field-by-field validation
        assertEquals(expectedSchema.fields().size, actualTransformedSchema.fields().size)

        for (expectedField in expectedSchema.fields()) {
            val actualField = actualTransformedSchema.field(expectedField.name())
            assertEquals(expectedField.name(), actualField.name(), "Field should exist: ${expectedField.name()}")
            assertEquals(expectedField.schema().type(), actualField.schema().type(), "Type mismatch for ${expectedField.name()}")
            assertEquals(expectedField.schema().isOptional, actualField.schema().isOptional, "Optional setting mismatch for ${expectedField.name()}")

            // Validate complex type structures
            when (expectedField.schema().type()) {
                Schema.Type.ARRAY -> {
                    assertEquals(
                        expectedField.schema().valueSchema().type(), actualField.schema().valueSchema().type(),
                        "Array ${expectedField.name()} value type mismatch"
                    )

                    // If array contains structs, validate struct schema fields
                    if (expectedField.schema().valueSchema().type() == Schema.Type.STRUCT) {
                        val expectedStructSchema = expectedField.schema().valueSchema()
                        val actualStructSchema = actualField.schema().valueSchema()
                        assertEquals(
                            expectedStructSchema.fields().size, actualStructSchema.fields().size,
                            "Struct field count mismatch in array ${expectedField.name()}"
                        )

                        for (expectedStructField in expectedStructSchema.fields()) {
                            val actualStructField = actualStructSchema.field(expectedStructField.name())
                            assertEquals(
                                expectedStructField.name(), actualStructField.name(),
                                "Struct field ${expectedStructField.name()} should exist in array ${expectedField.name()}"
                            )
                            assertEquals(
                                expectedStructField.schema().type(), actualStructField.schema().type(),
                                "Struct field ${expectedStructField.name()} type mismatch in array ${expectedField.name()}"
                            )
                            assertEquals(
                                expectedStructField.schema().isOptional, actualStructField.schema().isOptional,
                                "Struct field ${expectedStructField.name()} optional mismatch in array ${expectedField.name()}"
                            )
                        }
                    }
                }
                Schema.Type.MAP -> {
                    assertEquals(
                        expectedField.schema().keySchema().type(), actualField.schema().keySchema().type(),
                        "Map ${expectedField.name()} key type mismatch"
                    )
                    assertEquals(
                        expectedField.schema().valueSchema().type(), actualField.schema().valueSchema().type(),
                        "Map ${expectedField.name()} value type mismatch"
                    )
                }
                else -> {
                    // Basic types already validated above
                }
            }
        }
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
        hasNoComplexTypes(sinkRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))

        // NULL BODY TEST IMPLEMENTATION:
        // When body is null in the input data, transformer only processes top-level fields
        // Manual construction of expected values using hard-coded schema as container
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

        // Expected result: no body_* fields except those with defaults from schema, metadata_service uses default, is_deleted=1
        val expectedValue = Struct(expectedSchema)
            .put("id", id)
            .put("account_id", account_id)
            .put("employee_id", employee_id)
            .put("event_created_at", event_created_at)
            .put("body_observer", true) // Avro schema default when body is processed but observer field is absent
            .put("metadata_correlation_id", metadata_correlation_id)
            .put("metadata_causation_id", metadata_causation_id)
            .put("metadata_executor_id", metadata_executor_id)
            .put("metadata_service", "Default-Service") // v2 data has no service field, uses default
            .put("test_array_of_structs", test_array_of_structs)
            .put("test_string_array", test_string_array)
            .put("test_array_of_arrays", test_array_of_arrays)
            .put("test_map", test_map)
            .put("is_deleted", 1)
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
        hasNoComplexTypes(sinkRecord)
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
            .put("is_deleted", 0)
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
        hasNoComplexTypes(sinkRecord)
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
        hasNoComplexTypes(sinkRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))

        val actualSchema = transformedRecord.valueSchema()
        val expectedValue = Struct(actualSchema).put("is_deleted", 1).put("_kafka_metadata_partition", "0")
            .put("_kafka_metadata_offset", "156").put("_kafka_metadata_timestamp", 1713922160L)
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
        // Use the target Avro schema file that defines the expected flattened structure
        return AvroSchema.fromJson(fileContent("com/cultureamp/employee-data.employees-v1-target-schema-clickhouse.avsc"))
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
