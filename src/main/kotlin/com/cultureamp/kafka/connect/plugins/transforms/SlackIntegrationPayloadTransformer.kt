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
            logger.error("Exception: ", e)
            logger.error("Record Received: " + record.value())
            throw e
        }
    }

    private fun targetPayload(sourceValue: Struct, targetSchema: Schema): Struct {
        val targetPayload = Struct(targetSchema)

        targetPayload
            .put("account_aggregate_id", accountAggregateId(sourceValue))
            .put("is_deleted", isDeleted(sourceValue))
            .put("status", status(sourceValue))

        with(oauthPayload(oauthResponseData(sourceValue))) {
            targetPayload
                .put("access_token", accessToken)
                .put("team_id", teamId)
                .put("team_name", teamName)
                .put("access_token_scopes", scope)
                .put("enterprise_id", enterpriseId)
        }

        return targetPayload
    }

    private fun oauthPayload(oauthResponseData: Struct): OauthPayload {
        return try {
            // Only Slack Integration OAuth V1 has "bot" child element
            val bot = Requirements.requireStruct(oauthResponseData["bot"], purpose)

            OauthPayload(
                teamId = oauthResponseData["team_id"] as String,
                teamName = oauthResponseData["team_name"] as String,
                accessToken = bot["bot_access_token"] as String,
                scope = oauthResponseData["scope"] as String,
                enterpriseId = oauthResponseData["enterprise_id"] as String?
            )
        } catch (e: Exception) {
            // Slack Integration OAuth V2 Payload
            val team = Requirements.requireStruct(oauthResponseData["team"], purpose)

            OauthPayload(
                teamId = team["id"] as String,
                teamName = team["name"] as String,
                accessToken = oauthResponseData["access_token"] as String,
                scope = oauthResponseData["scope"] as String,
                enterpriseId = enterpriseId(oauthResponseData)
            )
        }
    }

    private fun enterpriseId(oauthResponseData: Struct): String? {
        return try {
            Requirements.requireStruct(oauthResponseData["enterprise"], purpose)["id"] as String
        } catch (e: Exception) {
            null
        }
    }

    private fun status(fullDocument: Struct): String = fullDocument["status"] as String

    private fun oauthResponseData(fullDocument: Struct): Struct =
        Requirements.requireStruct(fullDocument["oauth_response_data"], purpose)

    private fun accountAggregateId(fullDocument: Struct): String = fullDocument["account_aggregate_id"] as String
    private fun isDeleted(fullDocument: Struct): Boolean = fullDocument["deleted_at"] != null

    private fun targetSchema(): Schema {
        return SchemaBuilder.struct()
            .name("com.cultureamp.murmur.slack_integrations")
            .field("account_aggregate_id", Schema.STRING_SCHEMA)
            .field("access_token", Schema.STRING_SCHEMA)
            .field("team_id", Schema.STRING_SCHEMA)
            .field("team_name", Schema.STRING_SCHEMA)
            .field("access_token_scopes", Schema.STRING_SCHEMA)
            .field("enterprise_id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("is_deleted", Schema.BOOLEAN_SCHEMA)
            .field("status", Schema.STRING_SCHEMA)
            .build()
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
