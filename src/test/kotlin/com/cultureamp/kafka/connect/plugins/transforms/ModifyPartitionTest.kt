package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.source.SourceRecord
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ModifyPartitionTest {

    private fun configure(vararg props: Pair<String, Any>): ModifyPartition<SourceRecord> {
        val transformProperties = mutableMapOf<String, Any>()
        for (prop in props) {
            transformProperties[prop.first] = prop.second
        }
        val partitionSmt: ModifyPartition<SourceRecord> = ModifyPartition()
        partitionSmt.configure(transformProperties)

        return partitionSmt
    }

    private fun headers(vararg headers: Pair<String, String>): ConnectHeaders {
        val messageHeaders = ConnectHeaders()
        headers.forEach { messageHeaders.addString(it.first, it.second) }
        return messageHeaders
    }

    @Test
    fun `a missing header key should not result in a null pointer exception, but a connect exception`() {
        val partitionSmt = configure("header.key" to "account_id", "number.partitions" to 10)
        val record = SourceRecord(
            null,
            null,
            "test",
            0,
            null,
            null,
            null,
            "",
            789L,
            headers()
        )

        assertFailsWith<ConnectException> {
            partitionSmt.apply(record)
        }
    }

    @Test
    fun `calculate the partition number using the account_id header`() {
        val partitionSmt = configure("header.key" to "account_id", "number.partitions" to 10)
        val record = SourceRecord(
            null,
            null,
            "test",
            0,
            null,
            null,
            null,
            "",
            789L,
            headers("account_id" to "04a96f30-3dfa-11ec-9bbc-0242ac130002") // expected = 7 (manually calculated))
        )

        val transformedRecord: SourceRecord = partitionSmt.apply(record)
        assertEquals(transformedRecord.kafkaPartition(), 7)
    }

    @Test
    fun `fail if number of partitions is == 0`() {
        val partitionSmt = configure("header.key" to "account_id", "number.partitions" to 0)
        val record = SourceRecord(
            null,
            null,
            "test",
            0,
            null,
            null,
            null,
            "",
            789L,
            headers("account_id" to "04a96f30-3dfa-11ec-9bbc-0242ac130002") // expected = 7 (manually calculated))
        )

        assertFailsWith<ConnectException> {
            partitionSmt.apply(record)
        }
    }
}
