package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.transforms.Transformation
import org.slf4j.LoggerFactory

class UnquoteRecordKey<R : ConnectRecord<R>> : Transformation<R> {

    companion object {
        val CONFIG_DEF: ConfigDef = ConfigDef()
        private val logger = LoggerFactory.getLogger(UnquoteRecordKey::class.java)
    }

    override fun configure(configs: MutableMap<String, *>?) {}

    override fun close() {}

    override fun apply(record: R): R {
        val key = record.key()
        if (key == null || key !is String) {
            return record
        }

        val stripped = key.removeSurrounding("\"")
        if (stripped == key) {
            return record
        }

        logger.debug("Stripped surrounding quotes from key. Topic: {}, Original: {}, Stripped: {}", record.topic(), key, stripped)
        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            stripped,
            record.valueSchema(),
            record.value(),
            record.timestamp(),
        )
    }

    override fun config(): ConfigDef = CONFIG_DEF
}
