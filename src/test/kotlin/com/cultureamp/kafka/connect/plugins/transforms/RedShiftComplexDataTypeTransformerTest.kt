package com.cultureamp.kafka.connect.plugins.transforms

import com.mongodb.kafka.connect.source.MongoSourceConfig
import com.mongodb.kafka.connect.source.json.formatter.JsonWriterSettingsProvider
import com.mongodb.kafka.connect.source.schema.AvroSchema
import com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue
import com.mongodb.kafka.connect.util.ClassHelper
import com.mongodb.kafka.connect.util.ConfigHelper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.junit.Before
import java.io.File
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 *
 * A generic custom transform for RedShift
 *
 * This transformer class manipulates fields in schemas by turning them from arrays into strings
 * which is necessary for this:
 * https://stackoverflow.com/questions/61360342/kafka-connect-flatten-transformation-of-a-postgres-record-with-array-field issue to be solved
 * as RedShift does not support array types and arrays must be converted into strings.
 * See https://docs.confluent.io/platform/current/connect/javadocs/javadoc/org/apache/kafka/connect/transforms/Transformation.html.
 *
 * @param R is ConnectRecord<R>.
 * @constructor Creates a RedShiftComplexDataTypeTransformer Transformation<R> for a given ConnectRecord<T>
 *
 */
class RedShiftComplexDataTypeTransformerTest {

    private lateinit var transformer: RedShiftComplexDataTypeTransformer<SourceRecord>

    private fun hasNoComplexTypes(obj: SourceRecord): Boolean {

        var hasNoComplexTypes = true
        for (field in obj.valueSchema().fields()) {
            if (field.schema().type() == Schema.Type.ARRAY || field.schema().type() == Schema.Type.MAP || field.schema().type() == Schema.Type.STRUCT) {
                hasNoComplexTypes = false
            }
        }
        return hasNoComplexTypes
    }

    @Before
    fun setUp() {
        transformer = RedShiftComplexDataTypeTransformer()
    }

    @Test
    fun `can transform ECST Employee data that has arrays into string fields`() {

        val avroRecord = payload("com/cultureamp/employee-data.employees-v1.json")
        val sourceRecord = SourceRecord(
            null,
            null,
            "employee data ecst test",
            avroRecord.schema(),
            avroRecord.value()
        )

        val transformedRecord = transformer.apply(sourceRecord)
        hasNoComplexTypes(sourceRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))

        val expectedValue = struct(
            id, account_id, employee_id, event_created_at, body_source, body_employee_id, body_email, body_name, body_preferred_name, body_locale, body_observer, body_gdpr_erasure_request_id, body_test_map, body_test_map_1, body_test_array_of_structs, body_manager_assignment_manager_id, body_manager_assignment_demographic_id, body_erased, body_created_at, body_updated_at, body_deleted_at, metadata_correlation_id, metadata_causation_id, metadata_executor_id, metadata_service, test_array_of_structs, test_string_array, test_array_of_arrays, test_map,
            topic_key = "",
            tombstone = false
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(getExpectedSchema(), transformedRecord.valueSchema())
    }

    @Test
    fun `can transform ECST Employee data with null body`() {

        val avroRecord = payload("com/cultureamp/employee-data.employees-v2.json")
        val sourceRecord = SourceRecord(
            null,
            null,
            "employee data ecst test",
            avroRecord.schema(),
            avroRecord.value()
        )

        val transformedRecord = transformer.apply(sourceRecord)
        hasNoComplexTypes(sourceRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))

        val expectedValue = nullBodyStruct(
            id, account_id, employee_id, event_created_at, metadata_correlation_id, metadata_causation_id, metadata_executor_id, "Default-Service", test_array_of_structs, test_string_array, test_array_of_arrays, test_map,
            topic_key = "",
            tombstone = true
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(getExpectedSchema(), transformedRecord.valueSchema())
    }

    @Test
    fun `can transform ECST Employee data that has key as field`() {

        val avroRecord = payload("com/cultureamp/employee-data.employees-v1.json")
        val sourceRecord = SourceRecord(
            null,
            null,
            "employee data ecst test",
            null,
            Schema.STRING_SCHEMA,
            "hellp",
            avroRecord.schema(),
            avroRecord.value()
        )

        val transformedRecord = transformer.apply(sourceRecord)
        hasNoComplexTypes(sourceRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))

        val expectedValue = struct(
            id, account_id, employee_id, event_created_at, body_source, body_employee_id, body_email, body_name, body_preferred_name, body_locale, body_observer, body_gdpr_erasure_request_id, body_test_map, body_test_map_1, body_test_array_of_structs, body_manager_assignment_manager_id, body_manager_assignment_demographic_id, body_erased, body_created_at, body_updated_at, body_deleted_at, metadata_correlation_id, metadata_causation_id, metadata_executor_id, metadata_service, test_array_of_structs, test_string_array, test_array_of_arrays, test_map,
            topic_key = "hellp",
            tombstone = false
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(getExpectedSchema(), transformedRecord.valueSchema())
    }

    @Test
    fun `can transform ECST Employee data with tombstone message and non-null key`() {

        val avroRecord = payload("com/cultureamp/employee-data.employees-v1.json")
        val sourceRecord = SourceRecord(
            null,
            null,
            "employee data ecst test",
            null,
            Schema.STRING_SCHEMA,
            "hellp",
            avroRecord.schema(),
            null
        )

        val transformedRecord = transformer.apply(sourceRecord)
        hasNoComplexTypes(sourceRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))
    }

    @Test
    fun `can transform ECST Employee data with tombstone message and null key`() {

        val avroRecord = payload("com/cultureamp/employee-data.employees-v1.json")
        val sourceRecord = SourceRecord(
            null,
            null,
            "employee data ecst test",
            null,
            null,
            null,
            avroRecord.schema(),
            null
        )

        val transformedRecord = transformer.apply(sourceRecord)
        hasNoComplexTypes(sourceRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))

        val expectedValue = Struct(getExpectedSchema()).put("tombstone", true)

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(getExpectedSchema(), transformedRecord.valueSchema())
    }

    @Test
    fun `can transform ECST Employee data with tombstone message and null key and null value schema`() {
        val sourceRecord = SourceRecord(
            null,
            null,
            "employee data ecst test",
            null,
            null,
            null,
            null,
            null
        )

        val transformedRecord = transformer.apply(sourceRecord)
        assertTrue(hasNoComplexTypes(transformedRecord))
    }

    private val sourceSchema = AvroSchema.fromJson(fileContent("com/cultureamp/employee-data.employees-value-v1.avsc"))

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
    private val body_test_map = "{\"added_users_count\":\"0\",\"ignored_new_demographics_count\":\"0\",\"ignored_users_count\":\"0\",\"inactive_updated_users_count\":\"0\",\"reactivated_users_count\":\"0\",\"removed_users_count\":\"0\",\"updated_users_count\":\"0\"}"
    private val body_test_map_1 = "\"{}\""
    private val body_test_array_of_structs = "[{\"demographic_id\":\"{\\\"string\\\": \\\"[5c579970-684e-4911-a077-6bf407fb478d\\\"}\",\"demographic_value_id\":\"{\\\"string\\\": \\\"427b936f-e932-4673-95a2-acd3e3b900b1\\\"}\"},{\"demographic_id\":\"{\\\"string\\\": \\\"460f6b2d-03c5-46cf-ba55-aa14477a12dc]\\\"}\",\"demographic_value_id\":\"{\\\"string\\\": \\\"ecc0db2e-486e-4f4a-a54a-db21673e1a2b\\\"}\"}]"
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
    private val test_array_of_structs = "[{\"demographic_id\":\"{\\\"string\\\": \\\"5c579970-684e-4911-a077-6bf407fb478d\\\"}\",\"demographic_value_id\":\"{\\\"string\\\": \\\"427b936f-e932-4673-95a2-acd3e3b900b1\\\"}\"},{\"demographic_id\":\"{\\\"string\\\": \\\"460f6b2d-03c5-46cf-ba55-aa14477a12dc\\\"}\",\"demographic_value_id\":\"{\\\"string\\\": \\\"ecc0db2e-486e-4f4a-a54a-db21673e1a2b\\\"}\"}]"
    private val test_string_array = "[\"a\",\"b\",\"c\"]"
    private val test_array_of_arrays = "[[\"a\",\"b\",\"c\"],[\"e\"],[\"f\",\"g\"]]"
    private val test_map = "{\"added_users_count\":0,\"ignored_new_demographics_count\":0,\"ignored_users_count\":0,\"inactive_updated_users_count\":0,\"reactivated_users_count\":0,\"removed_users_count\":0,\"updated_users_count\":0}"

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
        body_test_map: String,
        body_test_map_1: String,
        body_test_array_of_structs: String,
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
        test_array_of_structs: String,
        test_string_array: String,
        test_array_of_arrays: String,
        test_map: String,
        topic_key: String?,
        tombstone: Boolean
    ): Struct {
        val returnStruct = Struct(getExpectedSchema())
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
            .put("tombstone", tombstone)
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
        test_array_of_structs: String,
        test_string_array: String,
        test_array_of_arrays: String,
        test_map: String,
        topic_key: String?,
        tombstone: Boolean
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
            .put("tombstone", tombstone)
        if (topic_key != "")
            returnStruct.put("topic_key", topic_key)
        return returnStruct
    }

    private fun fileContent(fileName: String): String {
        val url = this.javaClass.classLoader
            .getResource(fileName) ?: throw IllegalArgumentException("$fileName is not found 1")

        return String(Files.readAllBytes(File(url.file).toPath()))
    }

    private fun convertFieldSchema(orig: Schema, optional: Boolean, defaultFromParent: Any?): Schema {
        val builder = SchemaUtil.copySchemaBasics(orig)
        if (optional)
            builder.optional()
        if (defaultFromParent != null)
            builder.defaultValue(defaultFromParent)
        return builder.build()
    }

    private fun getExpectedSchema(): Schema {
        val expectedSchema = AvroSchema.fromJson(fileContent("com/cultureamp/employee-data.employees-v1-target-schema.avsc"))
        val builder = SchemaUtil.copySchemaBasics(expectedSchema)
        for (field in expectedSchema.fields()) {
            if (field.name() == "body_observer")
                builder.field("body_observer", convertFieldSchema(SchemaBuilder.bool().build(), true, true))
            else if (field.name() == "metadata_service")
                builder.field("metadata_service", convertFieldSchema(SchemaBuilder.string().build(), true, "Default-Service"))
            else
                builder.field(field.name(), field.schema())
        }
        return builder.build()
    }

    private val jsonWriterSettings =
        ClassHelper.createInstance(
            MongoSourceConfig.OUTPUT_JSON_FORMATTER_CONFIG,
            "com.mongodb.kafka.connect.source.json.formatter.DefaultJson",
            JsonWriterSettingsProvider::class.java
        ).jsonWriterSettings
}
