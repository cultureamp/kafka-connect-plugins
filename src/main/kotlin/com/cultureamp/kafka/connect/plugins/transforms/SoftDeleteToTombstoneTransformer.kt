package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.HIGH
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.SimpleConfig
import org.slf4j.LoggerFactory

class SoftDeleteToTombstoneTransformer<R : ConnectRecord<R>> : Transformation<R> {
    private lateinit var fieldPath: List<String>

    companion object {
        const val FIELD_CONFIG = "field"
        const val FIELD_DEFAULT = "body.deleted_at"
        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, FIELD_DEFAULT, HIGH, "Dot-notation path to the soft-delete field.")
        private val logger = LoggerFactory.getLogger(SoftDeleteToTombstoneTransformer::class.java)
    }

    override fun configure(configs: MutableMap<String, *>?) {
        val config = SimpleConfig(CONFIG_DEF, configs)
        fieldPath = config.getString(FIELD_CONFIG).split(".")
    }

    override fun close() {}

    override fun apply(record: R): R {
        if (record.value() == null) {
            return record
        }

        val value = record.value()
        if (value !is Struct) {
            logger.warn("Record value is not a Struct, passing through unchanged. Topic: {}", record.topic())
            return record
        }

        val deletedAtValue = resolveFieldValue(value)
        return if (deletedAtValue != null) {
            logger.debug("Soft-deleted record detected, converting to tombstone. Topic: {}, Key: {}", record.topic(), record.key())
            record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, null, record.timestamp())
        } else {
            record
        }
    }

    private fun resolveFieldValue(struct: Struct): Any? {
        var current: Struct = struct
        for (i in 0 until fieldPath.size - 1) {
            current = current.getStruct(fieldPath[i]) ?: return null
        }
        return current.get(fieldPath.last())
    }

    override fun config(): ConfigDef {
        return CONFIG_DEF
    }
}
