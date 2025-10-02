package com.cultureamp.kafka.connect.plugins.transforms

import com.mongodb.kafka.connect.source.MongoSourceConfig
import com.mongodb.kafka.connect.source.json.formatter.JsonWriterSettingsProvider
import com.mongodb.kafka.connect.source.schema.AvroSchema
import com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue
import com.mongodb.kafka.connect.util.ClassHelper
import com.mongodb.kafka.connect.util.ConfigHelper
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.BeforeEach
import java.io.File
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

class SlackIntegrationPayloadTransformerTest {
    private lateinit var transformer: SlackIntegrationPayloadTransformer<SourceRecord>

    @BeforeEach
    fun setUp() {
        transformer = SlackIntegrationPayloadTransformer()
    }

    @Test
    fun `can transform inserts for legacy Slack integrations`() {
        val valueAndSchema = payload("com/cultureamp/slack-integration-insert-v1.json")

        val transformedRecord = transformer.apply(
            SourceRecord(
                null,
                null,
                "test",
                valueAndSchema.schema(),
                valueAndSchema.value()
            )
        )

        val expectedValue = struct(
            accountId, accessToken, teamId, teamName, scope,
            enterpriseId, isDeleted = false, status = "active"
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(expectedSchema, transformedRecord.valueSchema())
    }

    @Test
    fun `can transform inserts for legacy Slack integrations and upgraded mongodb 6`() {
        val valueAndSchema = payload("com/cultureamp/slack-integration-insert-v1-mongo6.json")

        val transformedRecord = transformer.apply(
            SourceRecord(
                null,
                null,
                "test",
                valueAndSchema.schema(),
                valueAndSchema.value()
            )
        )

        val expectedValue = struct(
            accountId, accessToken, teamId, teamName, scope,
            enterpriseId, isDeleted = false, status = "active"
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(expectedSchema, transformedRecord.valueSchema())
    }

    @Test
    fun `can transform updates for legacy Slack integrations`() {
        val valueAndSchema = payload("com/cultureamp/slack-integration-update-v1.json")

        val transformedRecord = transformer.apply(
            SourceRecord(
                null,
                null,
                "test",
                valueAndSchema.schema(),
                valueAndSchema.value()
            )
        )

        val expectedValue = struct(
            accountId, accessToken, teamId, teamName, scope,
            enterpriseId = null, isDeleted = true, status = "inactive"
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(expectedSchema, transformedRecord.valueSchema())
    }

    @Test
    fun `can transform updates for legacy Slack integrations and upgraded mongodb 6`() {
        val valueAndSchema = payload("com/cultureamp/slack-integration-update-v1-mongo6.json")

        val transformedRecord = transformer.apply(
            SourceRecord(
                null,
                null,
                "test",
                valueAndSchema.schema(),
                valueAndSchema.value()
            )
        )

        val expectedValue = struct(
            accountId, accessToken, teamId, teamName, scope,
            enterpriseId = null, isDeleted = true, status = "inactive"
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(expectedSchema, transformedRecord.valueSchema())
    }

    @Test
    fun `can transform inserts for Slack integrations`() {
        val valueAndSchema = payload("com/cultureamp/slack-integration-insert-v2.json")

        val transformedRecord = transformer.apply(
            SourceRecord(
                null,
                null,
                "test",
                valueAndSchema.schema(),
                valueAndSchema.value()
            )
        )

        val expectedValue = struct(
            accountId, accessToken, teamId, teamName, scope,
            enterpriseId, isDeleted = false, status = "active"
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(expectedSchema, transformedRecord.valueSchema())
    }

    @Test
    fun `can transform inserts for Slack integrations and upgraded mongodb`() {
        val valueAndSchema = payload("com/cultureamp/slack-integration-insert-v2-mongo6.json")

        val transformedRecord = transformer.apply(
            SourceRecord(
                null,
                null,
                "test",
                valueAndSchema.schema(),
                valueAndSchema.value()
            )
        )

        val expectedValue = struct(
            accountId, accessToken, teamId, teamName, scope,
            enterpriseId, isDeleted = false, status = "active"
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(expectedSchema, transformedRecord.valueSchema())
    }

    @Test
    fun `can transform updates for Slack integrations`() {
        val valueAndSchema = payload("com/cultureamp/slack-integration-update-v2.json")

        val transformedRecord = transformer.apply(
            SourceRecord(
                null,
                null,
                "test",
                valueAndSchema.schema(),
                valueAndSchema.value()
            )
        )

        val expectedValue = struct(
            accountId, accessToken, teamId, teamName, scope,
            enterpriseId = null, isDeleted = true, status = "inactive"
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(expectedSchema, transformedRecord.valueSchema())
    }

    @Test
    fun `can transform updates for Slack integrations and upgraded mongodb 6`() {
        val valueAndSchema = payload("com/cultureamp/slack-integration-update-v2-mongo6.json")

        val transformedRecord = transformer.apply(
            SourceRecord(
                null,
                null,
                "test",
                valueAndSchema.schema(),
                valueAndSchema.value()
            )
        )

        val expectedValue = struct(
            accountId, accessToken, teamId, teamName, scope,
            enterpriseId = null, isDeleted = true, status = "inactive"
        )

        assertEquals(expectedValue, transformedRecord.value())
        assertEquals(expectedSchema, transformedRecord.valueSchema())
    }

    private val accessToken = "xoxb-2681941652837-3360300471349-5ANLxed6vDqNHhalK45O3RB8"
    private val accountId = "cd263a81-cd2d-4d88-b036-38681c57ea97"
    private val teamId = "T02L1TPK6QM"
    private val teamName = "Slack Testing"
    private val scope = "identity.basic,identity.email,identity.team"
    private val enterpriseId = "E03CFNSKHBM"

    private val sourceSchema = AvroSchema.fromJson(fileContent("com/cultureamp/slack-integration-source-schema.avsc"))
    private val expectedSchema = AvroSchema.fromJson(fileContent("com/cultureamp/slack-integration-target-schema.avsc"))

    private fun struct(
        accountId: String,
        accessToken: String,
        teamId: String,
        teamName: String,
        scope: String,
        enterpriseId: String?,
        isDeleted: Boolean,
        status: String,
    ): Struct {
        return Struct(expectedSchema)
            .put("account_aggregate_id", accountId)
            .put("access_token", accessToken)
            .put("team_id", teamId)
            .put("team_name", teamName)
            .put("access_token_scopes", scope)
            .put("enterprise_id", enterpriseId)
            .put("is_deleted", isDeleted)
            .put("status", status)
    }

    private fun payload(fileName: String): SchemaAndValue {
        val document = ConfigHelper.documentFromString(fileContent(fileName)).get()

        return BsonValueToSchemaAndValue(jsonWriterSettings)
            .toSchemaAndValue(sourceSchema, document.toBsonDocument())
    }

    private fun fileContent(fileName: String): String {
        val url = this.javaClass.classLoader
            .getResource(fileName) ?: throw IllegalArgumentException("$fileName is not found 1")

        return String(Files.readAllBytes(File(url.file).toPath()))
    }

    private val jsonWriterSettings =
        ClassHelper.createInstance(
            MongoSourceConfig.OUTPUT_JSON_FORMATTER_CONFIG,
            "com.mongodb.kafka.connect.source.json.formatter.DefaultJson",
            JsonWriterSettingsProvider::class.java
        ).jsonWriterSettings
}
