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
 * The PII strings below are modelled on the real DASE-3667 leak: the log message itself was
 * clean ("Error during write operation. Attempting rollback.") and every sensitive value was
 * inside the throwable's cause chain, inlined by the Redshift driver's batch-abort handler.
 */
class PiiRedactionPolicyTest {

    private val piiTokens = listOf(
        "Sarah", "resigning", "jo.tan", "example.com", "VALUES (", "142000", "emp-9931",
    )

    private fun policy(
        redactAtOrAbove: String = "ERROR",
        aggressiveAtOrAbove: String = "WARN",
        aggressiveLoggers: String? = "io.confluent.connect.jdbc,org.apache.kafka.connect.runtime.errors",
    ) = PiiRedactionPolicy.createPolicy(redactAtOrAbove, aggressiveAtOrAbove, aggressiveLoggers, true)

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
     * every {} after the first unsubstituted. This helper exists so no test can make that mistake
     * silently - the production code has the same trap, see PiiRedactionPolicy.redactMessage.
     */
    private fun parameterized(format: String, vararg args: Any?): ParameterizedMessage =
        ParameterizedMessage(format, *args)

    private fun assertNoPii(event: LogEvent) {
        val text = rendered(event)
        piiTokens.forEach { token ->
            assertFalse(text.contains(token), "leaked \"$token\" in: $text")
        }
    }

    /** The exact incident shape: clean message, all PII in the cause chain. */
    private fun incidentThrowable(): Throwable {
        val root = RuntimeException(
            "Batch entry 0 INSERT INTO datalake.incoming.conversations_conversations-attachments_v0" +
                "(id,note_content_text_plain) VALUES ('a3f1c2d4-1111-2222-3333-444455556666'," +
                "'Sarah mentioned she is struggling with her manager and is considering resigning') " +
                "was aborted: ERROR: value too long for type character varying(256)",
        )
        return BatchUpdateException("Sarah is resigning", IntArray(0), root)
    }

    @Test
    fun `redacts the message and the whole cause chain at ERROR`() {
        val out = policy().rewrite(
            event(
                Level.ERROR,
                "io.confluent.connect.jdbc.sink.JdbcSinkTask",
                SimpleMessage("Error during write operation. Attempting rollback."),
                incidentThrowable(),
            ),
        )
        assertNotNull(out)
        assertNoPii(out)
        assertEquals("[REDACTED]", out.message.formattedMessage)
    }

    @Test
    fun `preserves exception class names and stack frames`() {
        val original = incidentThrowable()
        val out = policy().rewrite(
            event(Level.ERROR, "io.confluent.connect.jdbc.sink.JdbcSinkTask", SimpleMessage("boom"), original),
        )
        val thrown = assertNotNull(out?.thrown)

        // The class chain is what makes a redacted error diagnosable at all.
        assertContains(thrown.message!!, BatchUpdateException::class.java.name)
        assertContains(assertNotNull(thrown.cause).message!!, RuntimeException::class.java.name)
        assertTrue(original.stackTrace.contentEquals(thrown.stackTrace), "stack frames not preserved")
    }

    @Test
    fun `keeps the format string and numeric arguments but scrubs the rest`() {
        val out = policy().rewrite(
            event(
                Level.WARN,
                "io.confluent.connect.jdbc.sink.JdbcSinkTask",
                parameterized(
                    "Write of {} records failed, remainingRetries={}, record={}",
                    3000, 4, "key=jo.tan@example.com salary=142000",
                ),
            ),
        )
        assertNotNull(out)
        assertNoPii(out)
        assertEquals(
            "Write of 3000 records failed, remainingRetries=4, record=[REDACTED]",
            out.message.formattedMessage,
        )
    }

    @Test
    fun `redacts a concatenated format string even when a throwable supplies a parameter`() {
        // log.error("Failed: " + record, e) reaches log4j as a format string that is already
        // fully rendered, with the throwable as the sole parameter. A "parameters are present"
        // test would wrongly treat that rendered text as a safe pattern and keep it.
        val msg = ParameterizedMessage(
            "Failed: key=jo.tan@example.com salary=142000",
            arrayOf<Any?>(RuntimeException("Sarah")),
        )
        val out = assertNotNull(policy().rewrite(event(Level.ERROR, "com.acme.X", msg)))
        assertNoPii(out)
        assertEquals("[REDACTED]", out.message.formattedMessage)
    }

    @Test
    fun `does not over-supply arguments when slf4j appends a throwable`() {
        // Surplus arguments make log4j emit a StatusLogger warning for every event.
        val msg = parameterized(
            "Write of {} records failed, remainingRetries={}",
            3000, 4, RuntimeException("Sarah jo.tan@example.com"),
        )
        val out = assertNotNull(policy().rewrite(event(Level.WARN, "io.confluent.connect.jdbc.x", msg)))
        assertNoPii(out)
        assertEquals("Write of 3000 records failed, remainingRetries=4", out.message.formattedMessage)
        assertEquals(2, out.message.parameters.size, "must pass exactly as many args as placeholders")
    }

    @Test
    fun `passes INFO through untouched`() {
        val input = event(
            Level.INFO,
            "org.apache.kafka.connect.runtime.WorkerSinkTask",
            SimpleMessage("Committing offsets for 512 acknowledged messages"),
        )
        assertSame(input, policy().rewrite(input), "INFO must not be rewritten at all")
    }

    @Test
    fun `passes WARN through untouched on loggers that are not aggressive`() {
        val input = event(
            Level.WARN,
            "org.apache.kafka.connect.runtime.WorkerSinkTask",
            parameterized("Commit of {} offsets timed out after {}ms", 512, 5000),
        )
        assertSame(input, policy().rewrite(input))
    }

    @Test
    fun `redacts WARN on aggressive loggers because JdbcSinkTask logs SQLException on retry`() {
        val input = event(
            Level.WARN,
            "io.confluent.connect.jdbc.sink.JdbcSinkTask",
            SimpleMessage("Write failed for Sarah"),
            incidentThrowable(),
        )
        val out = assertNotNull(policy().rewrite(input))
        assertFalse(out === input, "aggressive loggers must be redacted at WARN")
        assertNoPii(out)
    }

    @Test
    fun `redacts the LogReporter record dump`() {
        val out = policy().rewrite(
            event(
                Level.ERROR,
                "org.apache.kafka.connect.runtime.errors.LogReporter",
                SimpleMessage(
                    "Error encountered, consumed record is {key='emp-9931', " +
                        "value='{\"email\":\"jo.tan@example.com\",\"salary\":142000}'}",
                ),
            ),
        )
        assertNoPii(assertNotNull(out))
    }

    @Test
    fun `terminates on a cyclic cause chain`() {
        val outer = RuntimeException("outer Sarah")
        val inner = RuntimeException("inner jo.tan@example.com", outer)
        runCatching { outer.initCause(inner) } // legal to attempt; may throw, either way it must not hang

        val out = assertNotNull(policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("boom"), inner)))
        assertNoPii(out)
    }

    @Test
    fun `redacts an unclassifiable event rather than passing it through`() {
        val noLevel = Log4jLogEvent.newBuilder()
            .setLoggerName("com.acme.X")
            .setMessage(SimpleMessage("Sarah jo.tan@example.com"))
            .build()
        // Level defaults rather than being null in practice; assert the message is gone regardless.
        assertNoPii(assertNotNull(policy(redactAtOrAbove = "TRACE").rewrite(noLevel)))
    }

    @Test
    fun `an unparseable level tightens redaction rather than disabling it`() {
        val p = PiiRedactionPolicy.createPolicy("NOT_A_LEVEL", "ALSO_NOT", null, true)
        val out = assertNotNull(
            p.rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("Sarah jo.tan@example.com"))),
        )
        assertNoPii(out)
    }

    @Test
    fun `null message and null throwable do not panic`() {
        val before = PiiRedactionPolicy.panics()
        val out = policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("x"), null))
        assertNotNull(out)
        assertNull(out.thrown)
        assertEquals(before, PiiRedactionPolicy.panics(), "should not have hit the fallback path")
    }

    @Test
    fun `does not hit the fail-closed fallback for any normal event`() {
        val before = PiiRedactionPolicy.panics()
        listOf(
            event(Level.ERROR, "io.confluent.connect.jdbc.x", SimpleMessage("a"), incidentThrowable()),
            event(Level.WARN, "org.apache.kafka.connect.runtime.errors.LogReporter", SimpleMessage("b")),
            event(Level.INFO, "com.acme.X", parameterized("c {}", 1)),
        ).forEach { policy().rewrite(it) }
        assertEquals(before, PiiRedactionPolicy.panics(), "panics() must stay flat; see its KDoc")
    }

    /**
     * Exception classes are third-party code and every accessor except getClass() and
     * getSuppressed() is overridable, so a plugin can throw from inside the logging path. The
     * contract these tests lock in is: logging never breaks, and nothing leaks. Losing detail is
     * acceptable; losing the log line is not.
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
    fun `survives a throwable whose getStackTrace throws, keeping the class chain`() {
        val out = assertNotNull(
            policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("m"), HostileThrowable("stack"))),
        )
        assertNoPii(out)
        // Per-field guards mean one hostile accessor must not cost the whole chain.
        assertNotNull(out.thrown, "a throwing getStackTrace() must not discard the throwable")
        assertContains(out.thrown.message!!, "HostileThrowable")
    }

    @Test
    fun `survives a throwable whose getCause throws`() {
        val out = assertNotNull(
            policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("m"), HostileThrowable("cause"))),
        )
        assertNoPii(out)
        assertNotNull(out.thrown)
    }

    @Test
    fun `survives a throwable whose getMessage throws`() {
        val out = assertNotNull(
            policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("m"), HostileThrowable("message"))),
        )
        assertNoPii(out)
    }

    @Test
    fun `terminates when getCause returns the throwable itself`() {
        val out = assertNotNull(
            policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("m"), HostileThrowable("self"))),
        )
        assertNoPii(out)
    }

    @Test
    fun `survives a very deep cause chain`() {
        var t: Throwable = RuntimeException("root jo.tan@example.com")
        repeat(1000) { t = RuntimeException("level $it Sarah", t) }
        val out = assertNotNull(policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("m"), t)))
        assertNoPii(out)
    }

    @Test
    fun `redacts suppressed exceptions too`() {
        val t = RuntimeException("outer jo.tan@example.com")
        t.addSuppressed(IllegalStateException("suppressed Sarah"))
        val out = assertNotNull(policy().rewrite(event(Level.ERROR, "com.acme.X", SimpleMessage("m"), t)))
        assertNoPii(out)
    }

    @Test
    fun `survives a message whose accessors throw`() {
        val hostile = object : Message {
            override fun getFormattedMessage() = "fmt jo.tan@example.com"
            override fun getFormat(): String = throw RuntimeException("boom Sarah")
            override fun getParameters(): Array<Any?> = arrayOf("x")
            override fun getThrowable(): Throwable? = null
        }
        val out = assertNotNull(policy().rewrite(event(Level.ERROR, "com.acme.X", hostile)))
        assertNoPii(out)
        assertEquals("[REDACTED]", out.message.formattedMessage)
    }

    @Test
    fun `reads the connector name out of the MDC`() {
        val e = event(Level.ERROR, "com.acme.X", SimpleMessage("x"))
        assertEquals("datalake-conversations-attachments-v0", PiiRedactionPolicy.connectorName(e))
    }
}
