package com.cultureamp.kafka.connect.plugins.logging

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.impl.Log4jLogEvent
import org.apache.logging.log4j.message.Message
import org.apache.logging.log4j.message.ParameterizedMessage
import org.apache.logging.log4j.message.SimpleMessage
import org.apache.logging.log4j.util.SortedArrayStringMap
import java.io.PrintWriter
import java.io.StringWriter
import java.sql.BatchUpdateException
import java.sql.SQLException
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Test class for PiiRedactionPolicy.
 *
 * The shapes below are taken from real production events. Two matter most, because they are why
 * redaction is chain-wide rather than scoped to the SQLException itself:
 *
 *  - outer ConnectException with a CLEAN message ("Exiting WorkerSinkTask due to unrecoverable
 *    exception.") and the inlined INSERT in its cause;
 *  - outer RetriableException whose OWN message is "java.sql.SQLException: Exception chain: ..."
 *    carrying the inlined INSERT, because Throwable(Throwable) copies cause.toString().
 *
 * JdbcSinkTask.getAllMessagesException also builds a NEW plain SQLException whose message is the
 * concatenation of the whole chain, so the payload escapes upward out of the typed exception.
 */
class PiiRedactionPolicyTest {

    private val piiTokens = listOf(
        "Sarah", "resigning", "jo.tan", "example.com", "VALUES (", "142000", "emp-9931",
    )

    private fun policy(untrustedLoggers: String? = "org.apache.kafka.connect.runtime.errors") =
        PiiRedactionPolicy.createPolicy(untrustedLoggers)

    private fun event(
        level: Level,
        logger: String,
        message: Message,
        thrown: Throwable? = null,
    ): LogEvent {
        val ctx = SortedArrayStringMap()
        ctx.putValue("connector.context", "[datalake-conversations-attachments-v0|task-0] ")
        return Log4jLogEvent.newBuilder()
            .setLevel(level)
            .setLoggerName(logger)
            .setMessage(message)
            .setThrown(thrown)
            .setContextData(ctx)
            .build()
    }

    private fun rendered(event: LogEvent): String {
        val trace = event.thrown?.let {
            StringWriter().also { sw -> it.printStackTrace(PrintWriter(sw)) }.toString()
        } ?: ""
        return event.message.formattedMessage + "\n" + trace
    }

    /**
     * ParameterizedMessage overloads both (String, Object...) and (String, Object), so passing a
     * Kotlin array without the spread operator binds to the single-argument overload and leaves
     * every {} after the first unsubstituted.
     */
    private fun parameterized(format: String, vararg args: Any?): ParameterizedMessage =
        ParameterizedMessage(format, *args)

    private fun assertNoPii(event: LogEvent) {
        val text = rendered(event)
        piiTokens.forEach { token ->
            assertFalse(text.contains(token), "leaked \"$token\" in: $text")
        }
    }

    /** The raw driver exception, as JdbcDbWriter logs it. */
    private fun batchAbort(): BatchUpdateException = BatchUpdateException(
        "Batch entry 0 INSERT INTO datalake.incoming.conversations_conversations-attachments_v0" +
            "(id,note_content_text_plain) VALUES ('a3f1c2d4-1111'," +
            "'Sarah mentioned she is struggling and is considering resigning') was aborted",
        IntArray(0),
        null,
    )

    /** Production shape: outer message clean, payload in the cause. */
    private fun cleanOuterPoisonedCause(): Throwable {
        val synthetic = SQLException("Exception chain:\n" + batchAbort())
        return org.apache.kafka.connect.errors.ConnectException(
            "Exiting WorkerSinkTask due to unrecoverable exception.",
            synthetic,
        )
    }

    /** Production shape: wrapper copied cause.toString(), so its OWN message carries the payload. */
    private fun poisonedOuter(): Throwable {
        val synthetic = SQLException("Exception chain:\n" + batchAbort())
        return org.apache.kafka.connect.errors.RetriableException(synthetic.toString(), synthetic)
    }

    // ---------------------------------------------------------------- the gate

    @Test
    fun `redacts when the payload is in the cause and the outer message is clean`() {
        val out = assertNotNull(
            policy().rewrite(
                event(
                    Level.ERROR,
                    "org.apache.kafka.connect.runtime.WorkerTask",
                    parameterized(
                        "{} Task threw an uncaught and unrecoverable exception.",
                        "WorkerSinkTask{id=falcon.datalake-careerpathways-competencies-v3-0}",
                    ),
                    cleanOuterPoisonedCause(),
                ),
            ),
        )
        assertNoPii(out)
        // The log message survives: which connector and task, and what happened.
        assertContains(
            out.message.formattedMessage,
            "WorkerSinkTask{id=falcon.datalake-careerpathways-competencies-v3-0}",
        )
        assertContains(out.message.formattedMessage, "uncaught and unrecoverable exception")
    }

    @Test
    fun `redacts when the wrapper copied the payload into its own message`() {
        val out = assertNotNull(
            policy().rewrite(
                event(
                    Level.ERROR,
                    "org.apache.kafka.connect.runtime.WorkerSinkTask",
                    parameterized(
                        "{} RetriableException from SinkTask:",
                        "WorkerSinkTask{id=production-au.datalake-anytime-feedback-feedbacks-v1-0}",
                    ),
                    poisonedOuter(),
                ),
            ),
        )
        assertNoPii(out)
    }

    @Test
    fun `redacts a bare driver exception with no wrapper`() {
        // JdbcDbWriter: log.error("Error during write operation. Attempting rollback.", e)
        val out = assertNotNull(
            policy().rewrite(
                event(
                    Level.ERROR,
                    "io.confluent.connect.jdbc.sink.JdbcDbWriter",
                    SimpleMessage("Error during write operation. Attempting rollback."),
                    batchAbort(),
                ),
            ),
        )
        assertNoPii(out)
    }

    @Test
    fun `redacts when the SQLException is buried below a non-SQL wrapper`() {
        // 12 of 36 production leaks had a bare java.lang.Throwable as the outermost type. Scoping
        // by outer type, or to BatchUpdateException, would have missed a third of them.
        val buried = Throwable("wrapper", RuntimeException("mid", batchAbort()))
        val out = assertNotNull(
            policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("boom"), buried)),
        )
        assertNoPii(out)
    }

    @Test
    fun `redacts a suppressed SQLException`() {
        val outer = RuntimeException("outer")
        outer.addSuppressed(batchAbort())
        val out = assertNotNull(
            policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("boom"), outer)),
        )
        assertNoPii(out)
    }

    @Test
    fun `redacts the LogReporter record dump, which has no exception at all`() {
        // errors.log.include.messages builds the record into the message by concatenation. No
        // throwable, so no type check can see it - hence the logger prefix.
        val out = assertNotNull(
            policy().rewrite(
                event(
                    Level.ERROR,
                    "org.apache.kafka.connect.runtime.errors.LogReporter",
                    SimpleMessage("Error encountered, consumed record is {key='emp-9931', value='jo.tan@example.com'}"),
                ),
            ),
        )
        assertNoPii(out)
        assertEquals("[REDACTED]", out.message.formattedMessage)
    }

    // ------------------------------------------------------- passes through

    @Test
    fun `passes a non-SQL error through completely untouched`() {
        val input = event(
            Level.ERROR,
            "org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer",
            parameterized(
                "Failed to close {} with type {}",
                "coordinator",
                "org.apache.kafka.connect.runtime.distributed.WorkerCoordinator",
            ),
            org.apache.kafka.common.errors.InterruptException(InterruptedException()),
        )
        assertSame(input, policy().rewrite(input), "no SQLException in chain: must not be rewritten")
    }

    @Test
    fun `passes a herder shutdown error through with its message intact`() {
        val input = event(
            Level.ERROR,
            "org.apache.kafka.connect.runtime.distributed.DistributedHerder",
            SimpleMessage("Uncaught exception in herder work thread, exiting:"),
            org.apache.kafka.connect.errors.ConnectException(
                "Failed to stop KafkaBasedLog. Exiting without cleanly shutting down it's producer and consumer.",
                InterruptedException(),
            ),
        )
        assertSame(input, policy().rewrite(input))
    }

    @Test
    fun `passes INFO and WARN through untouched`() {
        listOf(Level.INFO, Level.WARN, Level.DEBUG).forEach { level ->
            val input = event(
                level,
                "org.apache.kafka.connect.runtime.WorkerSinkTask",
                parameterized("Committing offsets for {} acknowledged messages", 512),
            )
            assertSame(input, policy().rewrite(input), "$level with no SQLException must pass through")
        }
    }

    @Test
    fun `redacts DEBUG when the chain has a SQLException`() {
        // JdbcSinkTask's exhausted-retries ERROR tells you to enable DEBUG for detail. The old
        // level-threshold design let that through unredacted; there are no levels now.
        val input = event(
            Level.DEBUG,
            "io.confluent.connect.jdbc.sink.JdbcSinkTask",
            SimpleMessage("Exception chain:"),
            batchAbort(),
        )
        val out = assertNotNull(policy().rewrite(input))
        assertFalse(out === input, "DEBUG with a SQLException must be redacted")
        assertNoPii(out)
    }

    // ------------------------------------------ redacted-path message handling

    @Test
    fun `preserves exception class names and stack frames`() {
        val original = cleanOuterPoisonedCause()
        val out = assertNotNull(
            policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("boom"), original)),
        )
        val thrown = assertNotNull(out.thrown)
        assertContains(thrown.message!!, "org.apache.kafka.connect.errors.ConnectException")
        assertContains(assertNotNull(thrown.cause).message!!, "java.sql.SQLException")
        assertTrue(original.stackTrace.contentEquals(thrown.stackTrace), "stack frames not preserved")
    }

    // ------------------------------------------------------------- resilience

    /**
     * Exception classes are third-party code and every accessor except getClass() and
     * getSuppressed() is overridable, so a plugin can throw from inside the logging path. The
     * contract: logging never breaks, and nothing leaks. Losing detail is acceptable.
     */
    private class HostileThrowable(private val mode: String) : RuntimeException("PII jo.tan@example.com") {
        override fun getStackTrace(): Array<StackTraceElement> =
            if (mode == "stack") throw RuntimeException("boom Sarah") else super.getStackTrace()

        override val cause: Throwable?
            get() = when (mode) {
                "cause" -> throw RuntimeException("boom jo.tan@example.com")
                "self" -> this
                else -> super.cause
            }

        override val message: String?
            get() = if (mode == "message") throw RuntimeException("boom Sarah") else super.message
    }

    @Test
    fun `fails closed when getCause throws while walking the chain`() {
        // chainHasSqlException cannot prove the chain is clean, so it must assume it is not.
        val out = assertNotNull(
            policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("m"), HostileThrowable("cause"))),
        )
        assertNoPii(out)
    }

    @Test
    fun `survives a throwable whose getStackTrace throws, keeping the class chain`() {
        val hostile = HostileThrowable("stack")
        hostile.addSuppressed(batchAbort()) // make the chain untrusted so it takes the redact path
        val out = assertNotNull(
            policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("m"), hostile)),
        )
        assertNoPii(out)
        assertNotNull(out.thrown, "a throwing getStackTrace() must not discard the throwable")
        assertContains(out.thrown.message!!, "HostileThrowable")
    }

    @Test
    fun `terminates when getCause returns the throwable itself`() {
        val out = assertNotNull(
            policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("m"), HostileThrowable("self"))),
        )
        assertNoPii(out)
    }

    @Test
    fun `terminates on a very deep cause chain`() {
        var t: Throwable = batchAbort()
        repeat(1000) { t = RuntimeException("level $it Sarah", t) }
        val out = assertNotNull(policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("m"), t)))
        assertNoPii(out)
    }

    @Test
    fun `survives a message whose accessors throw`() {
        // getFormat() throwing must not break logging. The message is not rewritten at all now,
        // so the guarantee here is only that the event survives and the throwable is sanitised.
        val hostile = object : Message {
            override fun getFormattedMessage() = "boom"
            override fun getFormat(): String = throw RuntimeException("hostile")
            override fun getParameters(): Array<Any?> = arrayOf("x")
            override fun getThrowable(): Throwable? = null
        }
        val before = PiiRedactionPolicy.panics()
        val out = assertNotNull(policy().rewrite(event(Level.ERROR, "com.acme.X", hostile, batchAbort())))
        assertContains(assertNotNull(out.thrown).message!!, "[REDACTED]")
        assertEquals(before, PiiRedactionPolicy.panics(), "must not have needed the fallback")
    }

    /**
     * ACCEPTED RESIDUAL, pinned deliberately.
     *
     * Log messages are never rewritten. Across 3,728 production error events over 13 days, zero
     * had a payload in the message field and 37+ had one in an exception message - so the message
     * machinery this class used to carry (format-string preservation, argument scrubbing, an
     * identifier allowlist) was removed as unnecessary, along with three bugs that lived in it.
     *
     * The cost is this: a call site that concatenates record data into a log message is NOT
     * covered, even when a SQLException is present. If that ever shows up in real logs, this test
     * is the one to change - and the fix is a message-redaction branch on the untrusted path, not
     * a return to per-argument scrubbing.
     *
     * LogReporter, the one known concatenating logger, is covered by untrustedLoggers instead.
     */
    @Test
    fun `does not redact a log message even on a SQL chain - accepted residual`() {
        val out = assertNotNull(
            policy().rewrite(
                event(
                    Level.ERROR,
                    "com.acme.SomePlugin",
                    SimpleMessage("Failed on record key=jo.tan@example.com"),
                    batchAbort(),
                ),
            ),
        )
        // Message passes through untouched - this is the documented limitation.
        assertEquals("Failed on record key=jo.tan@example.com", out.message.formattedMessage)
        // The throwable is still sanitised, which is where every measured leak actually was.
        assertContains(assertNotNull(out.thrown).message!!, "[REDACTED]")
        assertFalse(rendered(out).contains("VALUES ("), "exception payload must still be gone")
    }

    @Test
    fun `null message and null throwable do not panic`() {
        val before = PiiRedactionPolicy.panics()
        val out = policy().rewrite(
            event(Level.ERROR, "org.apache.kafka.connect.runtime.errors.LogReporter", SimpleMessage("x")),
        )
        assertNotNull(out)
        assertNull(out.thrown)
        assertEquals(before, PiiRedactionPolicy.panics(), "should not have hit the fallback path")
    }

    @Test
    fun `does not hit the fail-closed fallback for any normal event`() {
        val before = PiiRedactionPolicy.panics()
        listOf(
            event(Level.ERROR, "io.confluent.connect.jdbc.x", SimpleMessage("a"), poisonedOuter()),
            event(Level.ERROR, "org.apache.kafka.connect.runtime.errors.LogReporter", SimpleMessage("b")),
            event(Level.INFO, "com.acme.X", parameterized("c {}", 1)),
        ).forEach { policy().rewrite(it) }
        assertEquals(before, PiiRedactionPolicy.panics(), "panics() must stay flat; see its KDoc")
    }

    // ------------------------------------------------------------------ config

    @Test
    fun `an empty untrusted logger list still redacts SQL chains`() {
        val out = assertNotNull(
            policy(untrustedLoggers = null).rewrite(
                event(Level.ERROR, "com.acme.X", SimpleMessage("m"), batchAbort()),
            ),
        )
        assertNoPii(out)
    }
}
