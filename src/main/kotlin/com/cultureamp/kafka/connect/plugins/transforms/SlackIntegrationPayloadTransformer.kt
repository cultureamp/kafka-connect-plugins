package com.cultureamp.kafka.connect.plugins.transforms

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.slf4j.LoggerFactory

class SlackIntegrationPayloadTransformer<R : ConnectRecord<R>> : Transformation<R> {
    private val logger = LoggerFactory.getLogger(this::class.java.canonicalName)
    private val purpose = "slack integration payload transformer"

    override fun configure(configs: MutableMap<String, *>?) {}

    override fun config(): ConfigDef {
        return ConfigDef()
    }

    override fun close() {}

    override fun apply(record: R): R {
        try {
            val sourceValue = Requirements.requireStruct(record.value(), purpose)
            val targetSchema = targetSchema()
            val targetPayload = targetPayload(sourceValue, targetSchema)

            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                targetSchema,
                targetPayload,
                record.timestamp()
            )
        } catch (e: Exception) {
            logger.error("DataException: ", e)
            logger.error("Record Received: " + record.value())
            throw e
        }
    }

    private fun targetPayload(sourceValue: Struct, targetSchema: Schema): Struct? {
        val targetPayload = Struct(targetSchema)
            .put("operationType", operationType(sourceValue))
            .put("document_id", documentId(sourceValue))

        val fullDocument = fullDocument(sourceValue)
        if (fullDocument != null) {
            targetPayload
                .put("account_aggregate_id", getAccountAggregateId(fullDocument))
                .put("status", status(fullDocument))

            with(oauthPayload(oauthResponseData(fullDocument))) {
                targetPayload
                    .put("access_token", accessToken)
                    .put("team_id", teamId)
                    .put("team_name", teamName)
                    .put("access_token_scopes", scope)
                    .put("enterprise_id", enterpriseId)
            }
        }

        return targetPayload
    }

    private fun oauthPayload(oauthResponseData: Map<*, *>): OauthPayload {
        return try {
            // Only Slack Integration OAuth V1 has "bot" child element
            val bot = oauthResponseData["bot"] as Map<*, *>

            OauthPayload(
                teamId = oauthResponseData["team_id"] as String,
                teamName = oauthResponseData["team_name"] as String,
                accessToken = bot["bot_access_token"] as String,
                scope = oauthResponseData["scope"] as String,
                enterpriseId = oauthResponseData["enterprise_id"] as String?
            )
        } catch (e: Exception) {
            // Slack Integration OAuth V2 Payload
            val team = oauthResponseData["team"] as Map<*, *>

            OauthPayload(
                teamId = team["id"] as String,
                teamName = team["name"] as String,
                accessToken = oauthResponseData["access_token"] as String,
                scope = oauthResponseData["scope"] as String,
                enterpriseId = enterpriseId(oauthResponseData)
            )
        }
    }

    private fun enterpriseId(oauthResponseData: Map<*, *>): String? {
        return try {
            (oauthResponseData["enterprise"] as Map<*, *>)["id"] as String
        } catch (e: Exception) {
            null
        }
    }

    private fun status(fullDocument: Map<*, *>): String = fullDocument["status"] as String

    private fun oauthResponseData(fullDocument: Map<*, *>): Map<*, *> =
        fullDocument["oauth_response_data"] as Map<*, *>

    private fun getAccountAggregateId(fullDocument: Map<*, *>): String = fullDocument["account_aggregate_id"] as String

    private fun targetSchema(): Schema {
        return SchemaBuilder.struct()
            .name("com.cultureamp.murmur.slack_integrations")
            .field("operationType", Schema.STRING_SCHEMA)
            .field("document_id", Schema.STRING_SCHEMA)
            .field("account_aggregate_id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("access_token", Schema.OPTIONAL_STRING_SCHEMA)
            .field("team_id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("team_name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("access_token_scopes", Schema.OPTIONAL_STRING_SCHEMA)
            .field("status", Schema.OPTIONAL_STRING_SCHEMA)
            .field("enterprise_id", Schema.OPTIONAL_STRING_SCHEMA)
            .build()
    }

    private fun operationType(valueStruct: Struct): String = valueStruct.get("operationType").toString()

    private fun documentId(valueStruct: Struct): String {
        val documentKey = objectMapper.readValue(valueStruct.get("documentKey").toString(), Map::class.java)
        return (documentKey["_id"] as Map<*, *>)["\$oid"] as String
    }

    private fun fullDocument(valueStruct: Struct): Map<*, *>? {
        return if (valueStruct.get("fullDocument") != null) {
            objectMapper.readValue(valueStruct.get("fullDocument").toString(), Map::class.java)
        } else null
    }

    private val objectMapper = ObjectMapper()
}

data class OauthPayload(
    val teamId: String?,
    val teamName: String?,
    val accessToken: String?,
    val scope: String?,
    val enterpriseId: String?,
)
