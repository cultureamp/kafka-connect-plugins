package com.cultureamp.kafka.connect.plugins.transforms

import java.util.UUID

interface Digest {
    fun encode(value: String): Long?
}

/**
 * Given a string, calculates a hash using CRC32 as a hash function
 */
class CRC32 : Digest {
    private var digest: java.util.zip.CRC32? = null

    init {
        this.digest = java.util.zip.CRC32()
    }

    override fun encode(value: String): Long? {
        this.digest?.update(value.toByteArray())
        return this.digest?.value
    }
}

/**
 * Used to calculate the partition for a Kafka message.
 * Requires a hash function and the number of partitions.
 *
 * partition = digest(value) % partitionCount
 */
class Partitioner(private val digest: Digest, private val partitionCount: Int) {
    init {
        if (partitionCount == 0) {
            throw Error("Partition count should be greater than 0")
        }
    }

    fun partitionNumberFor(value: String): Int? {
        return digest.encode(value)?.fmod(partitionCount)?.toInt()
    }

    fun partitionNumberFor(uuid: UUID): Int? {
        return digest.encode(uuid.toString())?.fmod(partitionCount)?.toInt()
    }
}

// Modulo function on Longs
infix fun Long.fmod(other: Int) = ((this % other) + other) % other
