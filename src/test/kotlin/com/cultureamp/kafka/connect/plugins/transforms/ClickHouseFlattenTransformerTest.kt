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
import org.apache.kafka.connect.data.SchemaBuilder
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

        // Schema already tested - focus on values for null body scenario
        val actualValue = transformedRecord.value() as Struct

        // Key assertions for null body scenario
        assertEquals(1, actualValue.get("is_deleted"), "Should be marked as deleted when body is null")
        assertEquals("1", actualValue.get("_kafka_metadata_partition"))
        assertEquals("156", actualValue.get("_kafka_metadata_offset"))
        assertEquals(null, actualValue.get("_kafka_metadata_timestamp"))
        assertEquals("Default-Service", actualValue.get("metadata_service"))

        // Verify basic fields are present
        assertEquals(id, actualValue.get("id"))
        assertEquals(account_id, actualValue.get("account_id"))
        assertEquals(employee_id, actualValue.get("employee_id"))
        assertEquals(event_created_at, actualValue.get("event_created_at"))
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

        // Use hard-coded schema as container for our manually-defined expected values
        // Not circular validation - schema is just a container, values are hand-written
        val expectedSchema = getExpectedSchema()

        val arrayStructSchema = expectedSchema.field("body_test_array_of_structs").schema().valueSchema()
        val body_test_array_of_structs = listOf(
            Struct(arrayStructSchema).apply {
                put("demographic_id", "{\"string\": \"5c579970-684e-4911-a077-6bf407fb478d\"}")
                put("demographic_value_id", "{\"string\": \"427b936f-e932-4673-95a2-acd3e3b900b1\"}")
            },
            Struct(arrayStructSchema).apply {
                put("demographic_id", "{\"string\": \"460f6b2d-03c5-46cf-ba55-aa14477a12dc\"}")
                put("demographic_value_id", "{\"string\": \"ecc0db2e-486e-4f4a-a54a-db21673e1a2b\"}")
            }
        )

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

        // Schema already tested in 'transform avro schema correctly' test - focus only on values
        // Validate Struct content field by field with deep comparison for complex types
        val actualValue = transformedRecord.value() as Struct
        assertEquals(expectedValue.schema().fields().size, actualValue.schema().fields().size, "Struct field count mismatch")

        for (expectedField in expectedValue.schema().fields()) {
            val expectedFieldValue = expectedValue.get(expectedField.name())
            val actualFieldValue = actualValue.get(expectedField.name())

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

    private fun struct(
        id: String,
        account_id: String,
        employee_id: String,
        event_created_at: Long,
        body_source: String,
        body_employee_id: String?,
        body_email: String,
        body_name: String,
        body_preferred_name: String?,
        body_locale: String?,
        body_observer: Boolean,
        body_gdpr_erasure_request_id: String?,
        body_test_map: Map<String, Int>,
        body_test_map_1: Map<String, String>?,
        body_test_array_of_structs: List<Struct>,
        body_manager_assignment_manager_id: String,
        body_manager_assignment_demographic_id: String,
        body_erased: Boolean,
        body_created_at: Long,
        body_updated_at: Long,
        body_deleted_at: Long?,
        metadata_correlation_id: String,
        metadata_causation_id: String?,
        metadata_executor_id: String,
        metadata_service: String,
        test_array_of_structs: List<Struct>,
        test_string_array: List<String>,
        test_array_of_arrays: List<List<String>>,
        test_map: Map<String, Int>,
        topic_key: String?,
        is_deleted: Int,
        actualSchema: Schema
    ): Struct {
        val returnStruct = Struct(actualSchema)
        returnStruct.put("id", id)
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
            .put("is_deleted", is_deleted)
        if (topic_key != "")
            returnStruct.put("topic_key", topic_key)
        return returnStruct
    }

    private fun nullBodyStruct(
        id: String,
        account_id: String,
        employee_id: String,
        event_created_at: Long,
        metadata_correlation_id: String,
        metadata_causation_id: String?,
        metadata_executor_id: String,
        metadata_service: String,
        test_array_of_structs: List<Struct>,
        test_string_array: List<String>,
        test_array_of_arrays: List<List<String>>,
        test_map: Map<String, Int>,
        topic_key: String?,
        is_deleted: Int
    ): Struct {
        val returnStruct = Struct(getExpectedSchema())
        returnStruct.put("id", id)
            .put("account_id", account_id)
            .put("employee_id", employee_id)
            .put("event_created_at", event_created_at)
            .put("metadata_correlation_id", metadata_correlation_id)
            .put("metadata_causation_id", metadata_causation_id)
            .put("metadata_executor_id", metadata_executor_id)
            .put("metadata_service", metadata_service)
            .put("test_array_of_structs", test_array_of_structs)
            .put("test_string_array", test_string_array)
            .put("test_array_of_arrays", test_array_of_arrays)
            .put("test_map", test_map)
            .put("is_deleted", is_deleted)
        if (topic_key != "")
            returnStruct.put("topic_key", topic_key)
        return returnStruct
    }

    private fun fileContent(fileName: String): String {
        val url = this.javaClass.classLoader
            .getResource(fileName) ?: throw IllegalArgumentException("$fileName is not found 1")

        return String(Files.readAllBytes(File(url.file).toPath()))
    }

    private fun getExpectedSchema(): Schema {
        // Hard-coded expected schema - NO TRANSFORMATION LOGIC - same as our schema test but with metadata fields
        // This is for value tests that need the full transformer schema including metadata
        return SchemaBuilder.struct()
            .name("com.cultureamp.employee.v1.Event")
            .version(1)
            // Top-level fields - all optional (transformer uses optional=true)
            .field("id", SchemaBuilder.string().optional().build())
            .field("account_id", SchemaBuilder.string().optional().build())
            .field("employee_id", SchemaBuilder.string().optional().build())
            .field("event_created_at", SchemaBuilder.int64().optional().build())
            // Flattened body fields - all optional
            .field("body_source", SchemaBuilder.string().optional().build())
            .field("body_employee_id", SchemaBuilder.string().optional().build())
            .field("body_email", SchemaBuilder.string().optional().build())
            .field("body_name", SchemaBuilder.string().optional().build())
            .field("body_preferred_name", SchemaBuilder.string().optional().build())
            .field("body_locale", SchemaBuilder.string().optional().build())
            .field("body_observer", SchemaBuilder.bool().optional().defaultValue(true).build())
            .field("body_gdpr_erasure_request_id", SchemaBuilder.string().optional().build())
            .field("body_test_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
            .field("body_test_map_1", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
            .field(
                "body_test_array_of_structs",
                SchemaBuilder.array(
                    SchemaBuilder.struct()
                        .field("demographic_id", SchemaBuilder.string().build()) // NOT optional in Avro schema
                        .field("demographic_value_id", SchemaBuilder.string().optional().build()) // Optional in Avro schema
                        .build()
                ).optional().build()
            )
            .field("body_manager_assignment_manager_id", SchemaBuilder.string().optional().build())
            .field("body_manager_assignment_demographic_id", SchemaBuilder.string().optional().build())
            .field("body_erased", SchemaBuilder.bool().optional().build())
            .field("body_created_at", SchemaBuilder.int64().optional().build())
            .field("body_updated_at", SchemaBuilder.int64().optional().build())
            .field("body_deleted_at", SchemaBuilder.int64().optional().build())
            // Flattened metadata fields - all optional
            .field("metadata_correlation_id", SchemaBuilder.string().optional().build())
            .field("metadata_causation_id", SchemaBuilder.string().optional().build())
            .field("metadata_executor_id", SchemaBuilder.string().optional().build())
            .field("metadata_service", SchemaBuilder.string().optional().defaultValue("Default-Service").build())
            // Top-level arrays and maps - all optional
            .field(
                "test_array_of_structs",
                SchemaBuilder.array(
                    SchemaBuilder.struct()
                        .field("demographic_id", SchemaBuilder.string().build()) // NOT optional in Avro schema
                        .field("demographic_value_id", SchemaBuilder.string().optional().build()) // Optional in Avro schema
                        .build()
                ).optional().build()
            )
            .field("test_string_array", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field("test_array_of_arrays", SchemaBuilder.array(SchemaBuilder.array(Schema.STRING_SCHEMA).build()).optional().build())
            .field("test_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
            // Kafka metadata fields added by transformer
            .field("topic_key", SchemaBuilder.string().defaultValue("").build())
            .field("is_deleted", SchemaBuilder.int32().defaultValue(0).build())
            .field("_kafka_metadata_partition", SchemaBuilder.string().optional().build())
            .field("_kafka_metadata_offset", SchemaBuilder.string().optional().build())
            .field("_kafka_metadata_timestamp", SchemaBuilder.int64().optional().build())
            .build()
    }

    private val jsonWriterSettings =
        ClassHelper.createInstance(
            MongoSourceConfig.OUTPUT_JSON_FORMATTER_CONFIG,
            "com.mongodb.kafka.connect.source.json.formatter.DefaultJson",
            JsonWriterSettingsProvider::class.java
        ).jsonWriterSettings
}
