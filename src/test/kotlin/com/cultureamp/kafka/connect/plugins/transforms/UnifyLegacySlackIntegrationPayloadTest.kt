package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import kotlin.test.Test
import kotlin.test.assertEquals

class UnifyLegacySlackIntegrationPayloadTest {
    private val CREATED_AT = "2022-04-08T06:40:02.649Z"
    private val ACCESS_TOKEN = "xoxb-12345"
    private val ACCOUNT_ID = "account-id"
    private val TEAM_ID = "team-id"
    private val TEAM_NAME = "team-name"
    private val SCOPE = "scope"

    private fun createOAuthV1Payload(): Pair<Schema, Struct> {
        val botSchema = SchemaBuilder.struct()
            .field("bot_access_token", Schema.STRING_SCHEMA)
            .field("bot_user_id", Schema.STRING_SCHEMA)
            .build()

        val botStruct = Struct(botSchema)
            .put("bot_access_token", ACCESS_TOKEN)
            .put("bot_user_id", "UV8DT789F")

        val oauthResponseDataSchema = SchemaBuilder.struct()
            .field("access_token", Schema.STRING_SCHEMA)
            .field("team_id", Schema.STRING_SCHEMA)
            .field("team_name", Schema.STRING_SCHEMA)
            .field("scope", Schema.STRING_SCHEMA)
            .field("bot", botSchema)
            .build()

        val oauthResponseDataStruct = Struct(oauthResponseDataSchema)
            .put("access_token", "a-b-c")
            .put("team_id", TEAM_ID)
            .put("team_name", TEAM_NAME)
            .put("scope", SCOPE)
            .put("bot", botStruct)

        val payloadSchema = SchemaBuilder.struct()
            .field("account_aggregate_id", Schema.STRING_SCHEMA)
            .field("created_at", Schema.STRING_SCHEMA)
            .field("oauth_response_data", oauthResponseDataSchema)
            .build()

        val payloadStruct: Struct = Struct(payloadSchema)
            .put("oauth_response_data", oauthResponseDataStruct)
            .put("account_aggregate_id", ACCOUNT_ID)
            .put("created_at", CREATED_AT)

        return payloadSchema to payloadStruct
    }

    private fun createOAuthV2Payload(): Pair<Schema, Struct> {
        val teamSchema = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build()

        val teamStruct = Struct(teamSchema)
            .put("id", TEAM_ID)
            .put("name", TEAM_NAME)

        val oauthResponseDataSchema = SchemaBuilder.struct()
            .field("access_token", Schema.STRING_SCHEMA)
            .field("scope", Schema.STRING_SCHEMA)
            .field("team", teamSchema)
            .build()

        val oauthResponseDataStruct = Struct(oauthResponseDataSchema)
            .put("access_token", ACCESS_TOKEN)
            .put("scope", SCOPE)
            .put("team", teamStruct)

        val payloadSchema = SchemaBuilder.struct()
            .field("account_aggregate_id", Schema.STRING_SCHEMA)
            .field("oauth_response_data", oauthResponseDataSchema)
            .build()

        val payloadStruct: Struct = Struct(payloadSchema)
            .put("oauth_response_data", oauthResponseDataStruct)
            .put("account_aggregate_id", ACCOUNT_ID)

        return payloadSchema to payloadStruct
    }

    private fun oAuthV1ExpectedValue(): Pair<Schema, Struct> {
        val botSchema = SchemaBuilder.struct()
            .field("bot_user_id", Schema.STRING_SCHEMA)
            .build()

        val botStruct = Struct(botSchema)
            .put("bot_user_id", "UV8DT789F")

        val oauthResponseDataSchema = SchemaBuilder.struct()
            .field("bot", botSchema)
            .build()

        val oauthResponseDataStruct = Struct(oauthResponseDataSchema)
            .put("bot", botStruct)

        val expectedSchema = SchemaBuilder.struct()
            .name("com.cultureamp.murmur.slack_integrations")
            .field("created_at", Schema.STRING_SCHEMA)
            .field("oauth_response_data", oauthResponseDataSchema)
            .field("account_aggregate_id", Schema.STRING_SCHEMA)
            .field("access_token", Schema.STRING_SCHEMA)
            .field("team_id", Schema.STRING_SCHEMA)
            .field("team_name", Schema.STRING_SCHEMA)
            .field("scope", Schema.STRING_SCHEMA)
            .build()

        val expectedValue = Struct(expectedSchema)
            .put("created_at", CREATED_AT)
            .put("oauth_response_data", oauthResponseDataStruct)
            .put("account_aggregate_id", ACCOUNT_ID)
            .put("access_token", ACCESS_TOKEN)
            .put("team_id", TEAM_ID)
            .put("team_name", TEAM_NAME)
            .put("scope", SCOPE)

        return expectedSchema to expectedValue
    }

    @Test
    fun `With Legacy Slack Integration Data`() {
        val transformer: UnifyLegacySlackIntegrationPayload<SourceRecord> = UnifyLegacySlackIntegrationPayload()
        val (payloadSchema, payloadStruct) = createOAuthV1Payload()

        val transformedRecord: SourceRecord = transformer.apply(SourceRecord(null, null, "test", payloadSchema, payloadStruct))

        val (expectedSchema, expectedValue) = oAuthV1ExpectedValue()

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(expectedSchema, transformedRecord.valueSchema())
    }

    @Test
    fun `With Slack Integration Data`() {
        val transformer: UnifyLegacySlackIntegrationPayload<SourceRecord> = UnifyLegacySlackIntegrationPayload()

        val (payloadSchema, payloadStruct) = createOAuthV2Payload()

        val transformedRecord: SourceRecord = transformer.apply(SourceRecord(null, null, "test", payloadSchema, payloadStruct))

        val expectedSchema = SchemaBuilder.struct()
            .name("com.cultureamp.murmur.slack_integrations")
            .field("account_aggregate_id", Schema.STRING_SCHEMA)
            .field("access_token", Schema.STRING_SCHEMA)
            .field("team_id", Schema.STRING_SCHEMA)
            .field("team_name", Schema.STRING_SCHEMA)
            .field("scope", Schema.STRING_SCHEMA)
            .build()
        val expectedValue = Struct(expectedSchema)
            .put("account_aggregate_id", ACCOUNT_ID)
            .put("access_token", ACCESS_TOKEN)
            .put("team_id", TEAM_ID)
            .put("team_name", TEAM_NAME)
            .put("scope", SCOPE)

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(expectedSchema, transformedRecord.valueSchema())
    }
}
