package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements

class UnifyLegacySlackIntegrationPayload<R : ConnectRecord<R>> : Transformation<R> {
    val attributesToIgnoreList = arrayListOf("account_aggregate_id", "oauth_response_data.access_token", "oauth_response_data.team_id", "oauth_response_data.team_name", "oauth_response_data.bot.bot_access_token", "oauth_response_data.team.id", "oauth_response_data.team.name")
    val PURPOSE = "unify legacy slack integration data"
    override fun configure(configs: MutableMap<String, *>?) {}

    override fun close() {}

    fun removeIgnoredAttributes(fields: List<Field>, builder: SchemaBuilder, hierarchy: String = ""): SchemaBuilder {
        for (field in fields) {
            if ("$hierarchy${field.name()}" !in attributesToIgnoreList) {
                if (field.schema().type().getName() == "struct") {
                    val childSchema = removeIgnoredAttributes(field.schema().fields(), SchemaBuilder.struct(), "$hierarchy${field.name()}.")
                    // Only add child schema if is not empty
                    if (childSchema.fields().count() > 0) {
                        builder.field(field.name(), childSchema.build())
                    }
                } else {
                    builder
                        .field(field.name(), field.schema())
                }
            }
        }
        return builder
    }

    fun populateValue(originalValues: Struct, updatedValues: Struct): Struct {
        var newFields = updatedValues.schema().fields()
        for (field in newFields) {
            try {
                if (field.schema().type().getName() == "struct") {
                    val childValue = populateValue(Requirements.requireStruct(originalValues.get(field.name()), PURPOSE), Struct(field.schema()))
                    updatedValues.put(field.name(), childValue)
                } else {
                    updatedValues
                        .put(field.name(), originalValues.get(field.name()))
                }
            } catch (e: DataException) {
            }
        }
        return updatedValues
    }

    fun extractUnifiedValue(oauthResponseData: Struct): Triple<String, String, String> {
        var teamId: String
        var teamName: String
        var accessToken: String

        try {
            // Only Slack Integration OAuth V1 has "bot" child element
            val dot: Struct = Requirements.requireStruct(oauthResponseData.get("bot"), PURPOSE)
            teamId = oauthResponseData.get("team_id") as String
            teamName = oauthResponseData.get("team_name") as String
            accessToken = dot.get("bot_access_token") as String
        } catch (e: DataException) {
            // Slack Integration OAuth V2 Payload
            val team: Struct = Requirements.requireStruct(oauthResponseData.get("team"), PURPOSE)
            teamId = team.get("id") as String
            teamName = team.get("name") as String
            accessToken = oauthResponseData.get("access_token") as String
        }
        return Triple(teamId, teamName, accessToken)
    }

    override fun apply(record: R): R {
        val valueStruct: Struct = Requirements.requireStruct(record.value(), PURPOSE)
        val oauthResponseData: Struct = Requirements.requireStruct(valueStruct.get("oauth_response_data"), PURPOSE)
        val updatedSchemaBuilder: SchemaBuilder = removeIgnoredAttributes(valueStruct.schema().fields(), SchemaBuilder.struct())
        val(teamId, teamName, accessToken) = extractUnifiedValue(oauthResponseData)

        // Add back the unified fields
        val modifiedPayloadSchema = updatedSchemaBuilder
            .name("com.cultureamp.murmur.slack_integrations")
            .field("account_aggregate_id", Schema.STRING_SCHEMA)
            .field("access_token", Schema.STRING_SCHEMA)
            .field("team_id", Schema.STRING_SCHEMA)
            .field("team_name", Schema.STRING_SCHEMA)
            .build()

        val updatedValuesStruct: Struct = populateValue(valueStruct, Struct(modifiedPayloadSchema))

        val modifiedPayloadStruct = updatedValuesStruct
            .put("access_token", accessToken)
            .put("team_id", teamId)
            .put("team_name", teamName)

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), modifiedPayloadSchema, modifiedPayloadStruct, record.timestamp())
    }

    override fun config(): ConfigDef {
        return ConfigDef()
    }
}
