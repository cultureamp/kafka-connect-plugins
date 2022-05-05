package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements

data class Quintuple<T1, T2, T3, T4, T5>(val t1: T1, val t2: T2, val t3: T3, val t4: T4, val t5: T5)

class UnifyLegacySlackIntegrationPayload<R : ConnectRecord<R>> : Transformation<R> {
    private val ignoredAttributes = arrayListOf(
        "account_aggregate_id",
        "oauth_response_data.access_token",
        "oauth_response_data.team_id",
        "oauth_response_data.team_name",
        "oauth_response_data.enterprise_id",
        "oauth_response_data.scope",
        "oauth_response_data.bot.bot_access_token",
        "oauth_response_data.team.id",
        "oauth_response_data.team.name",
        "oauth_response_data.enterprise.id",
    )
    private val PURPOSE = "unify legacy slack integration data"
    override fun configure(configs: MutableMap<String, *>?) {}

    override fun close() {}

    private fun populateValue(originalValues: Struct, updatedValues: Struct): Struct {
        val newFields = updatedValues.schema().fields()
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
                // This is catch exception thrown when field.name() in .get(field.name())) does not exists
            }
        }
        return updatedValues
    }

    private fun extractUnifiedValues(oauthResponseData: Struct): Quintuple<String, String, String, String, String?> {
        var teamId: String
        var teamName: String
        var accessToken: String
        var scope: String
        var enterpriseId: String?

        try {
            // Only Slack Integration OAuth V1 has "bot" child element
            val bot: Struct = Requirements.requireStruct(oauthResponseData.get("bot"), PURPOSE)
            teamId = oauthResponseData.get("team_id") as String
            teamName = oauthResponseData.get("team_name") as String
            accessToken = bot.get("bot_access_token") as String
            scope = oauthResponseData.get("scope") as String
            enterpriseId = oauthResponseData.get("enterprise_id") as String?
        } catch (e: DataException) {
            System.out.println("oauthResponseData: $oauthResponseData")
            // Slack Integration OAuth V2 Payload
            val team: Struct = Requirements.requireStruct(oauthResponseData.get("team"), oauthResponseData.toString())
            teamId = team.get("id") as String
            teamName = team.get("name") as String
            accessToken = oauthResponseData.get("access_token") as String
            scope = oauthResponseData.get("scope") as String
            enterpriseId = extractEnterpriseValue(oauthResponseData)
        }
        return Quintuple(teamId, teamName, accessToken, scope, enterpriseId)
    }

    private fun extractEnterpriseValue(oauthResponseData: Struct): String? {
        try {
            return Requirements.requireStruct(oauthResponseData.get("enterprise"), PURPOSE).get("id") as String
        } catch (e: DataException) {
            return null
        }
    }

    override fun apply(record: R): R {
        val valueStruct: Struct = Requirements.requireStruct(record.value(), PURPOSE)
        val oauthResponseData: Struct = Requirements.requireStruct(valueStruct.get("oauth_response_data"), PURPOSE)
        val(teamId, teamName, accessToken, scope, enterpriseId) = extractUnifiedValues(oauthResponseData)
        var account_aggregate_id = valueStruct.get("account_aggregate_id") as String

        val modifiedPayloadSchema = SchemaBuilder.struct()
            .name("com.cultureamp.murmur.slack_integrations")
            .field("account_aggregate_id", Schema.STRING_SCHEMA)
            .field("access_token", Schema.STRING_SCHEMA)
            .field("team_id", Schema.STRING_SCHEMA)
            .field("team_name", Schema.STRING_SCHEMA)
            .field("access_token_scopes", Schema.STRING_SCHEMA)
            .field("enterprise_id", Schema.OPTIONAL_STRING_SCHEMA)
            .build()

        val modifiedPayloadStruct = Struct(modifiedPayloadSchema)
            .put("account_aggregate_id", account_aggregate_id)
            .put("access_token", accessToken)
            .put("team_id", teamId)
            .put("team_name", teamName)
            .put("access_token_scopes", scope)
            .put("enterprise_id", enterpriseId)

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), modifiedPayloadSchema, modifiedPayloadStruct, record.timestamp())
    }

    override fun config(): ConfigDef {
        return ConfigDef()
    }
}
