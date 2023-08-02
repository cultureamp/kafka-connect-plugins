package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
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

        val bytes = fileContent("com/cultureamp/employee-data.employees-value-v1.json")
        val parser = Schema.Parser()
        val schema = parser.parse(bytes)
        val avroRecord = GenericData.Record(schema)

        val transformedRecord = transformer.apply(
            SourceRecord(
                null,
                null,
                "employee data ecst test",
                avroRecord.schema(),
                avroRecord.value()
            )
        )

        assertTrue(hasNoArrays(transformedRecord))
    }

    private fun fileContent(fileName: String): String {
        val url = this.javaClass.classLoader
            .getResource(fileName) ?: throw IllegalArgumentException("$fileName is not found 1")

        return String(Files.readAllBytes(File(url.file).toPath()))
    }
}
