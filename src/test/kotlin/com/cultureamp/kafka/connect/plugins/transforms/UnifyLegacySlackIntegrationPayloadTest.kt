package com.cultureamp.kafka.connect.plugins.transforms

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import kotlin.test.Test
import kotlin.test.assertEquals

class UnifyLegacySlackIntegrationPayloadTest {

    fun createSlackIntegrationPayload(): Pair<Schema, Struct> {
        val botSchema = SchemaBuilder.struct()
            .field("bot_access_token", Schema.STRING_SCHEMA)
            .field("bot_user_id", Schema.STRING_SCHEMA)
            .build()

        val botStruct = Struct(botSchema)
            .put("bot_access_token", "xoxb-12345")
            .put("bot_user_id", "UV8DT789F")

        val oauthResponseDataSchema = SchemaBuilder.struct()
            .field("access_token", Schema.STRING_SCHEMA)
            .field("team_id", Schema.STRING_SCHEMA)
            .field("team_name", Schema.STRING_SCHEMA)
            .field("bot", botSchema)
            .build()

        val oauthResponseDataStruct = Struct(oauthResponseDataSchema)
            .put("access_token", "a-b-c")
            .put("team_id", "a-b-c")
            .put("team_name", "a-b-c")
            .put("bot", botStruct)

        val payloacSchema = SchemaBuilder.struct()
            .field("account_aggregate_id", Schema.STRING_SCHEMA)
            .field("created_at", Schema.STRING_SCHEMA)
            .field("oauth_response_data", oauthResponseDataSchema)
            .build()

        val payloadStruct: Struct = Struct(payloacSchema)
            .put("oauth_response_data", oauthResponseDataStruct)
            .put("account_aggregate_id", "1-2-3")
            .put("created_at", "2022-04-08T06:40:02.649Z")

        return payloacSchema to payloadStruct
    }

    fun createLagacySlackIntegrationPayload(): Pair<Schema, Struct> {
        val teamSchema = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build()

        val teamStruct = Struct(teamSchema)
            .put("id", "a-b-c")
            .put("name", "a-b-c")

        val oauthResponseDataSchema = SchemaBuilder.struct()
            .field("access_token", Schema.STRING_SCHEMA)
            .field("team", teamSchema)
            .build()

        val oauthResponseDataStruct = Struct(oauthResponseDataSchema)
            .put("access_token", "xoxb-12345")
            .put("team", teamStruct)

        val payloacSchema = SchemaBuilder.struct()
            .field("account_aggregate_id", Schema.STRING_SCHEMA)
            .field("oauth_response_data", oauthResponseDataSchema)
            .build()

        val payloadStruct: Struct = Struct(payloacSchema)
            .put("oauth_response_data", oauthResponseDataStruct)
            .put("account_aggregate_id", "1-2-3")

        return payloacSchema to payloadStruct
    }

    @Test
    fun `With slack Integration Data`() {
        val partitionSmt: UnifyLegacySlackIntegrationPayload<SourceRecord> = UnifyLegacySlackIntegrationPayload()
        val (payloacSchema, payloadStruct) = createSlackIntegrationPayload()

        val transformedRecord: SourceRecord = partitionSmt.apply(SourceRecord(null, null, "test", payloacSchema, payloadStruct))
        assertEquals(transformedRecord.value().toString(), "Struct{created_at=2022-04-08T06:40:02.649Z,oauth_response_data=Struct{bot=Struct{bot_user_id=UV8DT789F}},account_aggregate_id=1-2-3,access_token=xoxb-12345,team_id=a-b-c,team_name=a-b-c}")
    }

    @Test
    fun `With legacy Slack Integration Data`() {
        val partitionSmt: UnifyLegacySlackIntegrationPayload<SourceRecord> = UnifyLegacySlackIntegrationPayload()

        val (payloacSchema, payloadStruct) = createLagacySlackIntegrationPayload()

        val transformedRecord: SourceRecord = partitionSmt.apply(SourceRecord(null, null, "test", payloacSchema, payloadStruct))
        assertEquals(transformedRecord.value().toString(), "Struct{account_aggregate_id=1-2-3,access_token=xoxb-12345,team_id=a-b-c,team_name=a-b-c}")
    }
}
