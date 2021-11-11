package com.cultureamp.kafka.connect.plugins.transforms

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class PartitionerTest {
    private val partitionCount = 10

    @Test
    fun `calculate the partition number given a UUID`() {
        val uuid = UUID.fromString("04a96f30-3dfa-11ec-9bbc-0242ac130002")
        assertEquals(7, Partitioner(CRC32(), partitionCount).partitionNumberFor(uuid)) // Partition 7 manually calculated using CRC32
    }

    @Test
    fun `calculate the partition number given a string`() {
        assertEquals(0, Partitioner(CRC32(), partitionCount).partitionNumberFor("my-string"))
    }
}
