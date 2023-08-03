package com.cultureamp.kafka.connect.plugins.transforms

import com.mongodb.kafka.connect.source.MongoSourceConfig
import com.mongodb.kafka.connect.source.json.formatter.JsonWriterSettingsProvider
import com.mongodb.kafka.connect.source.schema.AvroSchema
import com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue
import com.mongodb.kafka.connect.util.ClassHelper
import com.mongodb.kafka.connect.util.ConfigHelper
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.source.SourceRecord
import org.junit.Before
import java.io.File
import java.nio.file.Files
import kotlin.test.Test
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
 * @constructor Creates a RedShiftArrayTransformer Transformation<R> for a given ConnectRecord<T>
 *
 */
class RedShiftArrayTransformerTest {

    private lateinit var transformer: RedShiftArrayTransformer<SourceRecord>

    private fun hasNoArrays(obj: Any): Boolean {
        var hasArray = false
        obj::class.members.forEach { member ->
            when {
                member is Array<*> -> hasArray = true
            }
        }
        return hasArray
    }

    @Before
    fun setUp() {
        transformer = RedShiftArrayTransformer()
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
        System.out.println(sourceRecord)
        System.out.println(transformedRecord)

        assertTrue(hasNoArrays(transformedRecord))
    }

    private val sourceSchema = AvroSchema.fromJson(fileContent("com/cultureamp/employee-data.employees-value-v1.avsc"))

    private fun payload(fileName: String): SchemaAndValue {
        val document = ConfigHelper.documentFromString(fileContent(fileName)).get()

        return BsonValueToSchemaAndValue(jsonWriterSettings)
            .toSchemaAndValue(sourceSchema, document.toBsonDocument())
    }

    private fun fileContent(fileName: String): String {
        val url = this.javaClass.classLoader
            .getResource(fileName) ?: throw IllegalArgumentException("$fileName is not found 1")

        return String(Files.readAllBytes(File(url.file).toPath()))
    }

    private val jsonWriterSettings =
        ClassHelper.createInstance(
            MongoSourceConfig.OUTPUT_JSON_FORMATTER_CONFIG,
            "com.mongodb.kafka.connect.source.json.formatter.DefaultJson",
            JsonWriterSettingsProvider::class.java
        ).jsonWriterSettings
}
