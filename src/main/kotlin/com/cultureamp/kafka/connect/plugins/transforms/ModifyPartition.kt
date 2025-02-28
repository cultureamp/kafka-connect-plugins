package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.HIGH
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.SimpleConfig

class ModifyPartition<R : ConnectRecord<R>> : Transformation<R> {
    private var headerKey: String? = null
    private var partitionCount: Int? = null

    companion object {
        const val HEADER_KEY = "header.key"
        const val NUMBER_OF_PARTITIONS = "number.partitions"
        val CONFIG_DEV: ConfigDef = ConfigDef()
            .define(HEADER_KEY, ConfigDef.Type.STRING, null, HIGH, "Key used to extract the partition key from the record headers.")
            .define(NUMBER_OF_PARTITIONS, ConfigDef.Type.INT, null, HIGH, "Number of topic partitions")
    }

    override fun configure(configs: MutableMap<String, *>?) {
        val config = SimpleConfig(CONFIG_DEV, configs)
        this.headerKey = config.getString(HEADER_KEY)
        this.partitionCount = config.getInt(NUMBER_OF_PARTITIONS)
    }

    override fun close() {}

    override fun apply(record: R): R {
        if (partitionCount == null) {
            throw ConnectException("The property `$NUMBER_OF_PARTITIONS` must be set.")
        } else if (partitionCount!! <= 0) {
            throw ConnectException("Partition count should be greater than 0")
        }
        val userSpecifiedPartitionKey = record.headers().lastWithName(headerKey)?.value() as String?

        if (userSpecifiedPartitionKey != null) {
            val partitionNumber = Partitioner(CRC32(), partitionCount!!).partitionNumberFor(userSpecifiedPartitionKey)
            return record.newRecord(record.topic(), partitionNumber, record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp(), record.headers())
        } else {
            throw ConnectException("Failed to determine partition key using header key $headerKey")
        }
    }

    override fun config(): ConfigDef {
        return CONFIG_DEV
    }
}
