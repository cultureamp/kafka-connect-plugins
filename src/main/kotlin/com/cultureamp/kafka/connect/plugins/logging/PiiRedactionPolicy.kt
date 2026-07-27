package com.cultureamp.kafka.connect.plugins.logging

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.Core
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.appender.rewrite.RewritePolicy
import org.apache.logging.log4j.core.config.plugins.Plugin
import org.apache.logging.log4j.core.config.plugins.PluginAttribute
import org.apache.logging.log4j.core.config.plugins.PluginFactory
import org.apache.logging.log4j.core.impl.Log4jLogEvent
import org.apache.logging.log4j.message.Message
import org.apache.logging.log4j.message.ParameterizedMessage
import org.apache.logging.log4j.message.SimpleMessage
import org.apache.logging.log4j.status.StatusLogger
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Structural PII redaction for Kafka Connect logs.
 *
 * A log4j2 [RewritePolicy] that decides what to redact from the [LogEvent] object graph rather
 * than by pattern-matching rendered text. Intended to replace `io.confluent.log4j2.redactor
 * .RedactorPolicy`, whose regex rule file has three properties this does not:
 *
 * 1. **It fails open.** `StringRedactorEngine` catches a rules-compile error, logs one line and
 *    installs an empty redactor, so a typo in the rules file silently disables redaction
 *    entirely. Here the default is to redact, and [rewrite] catches its own bugs and degrades
 *    to full redaction rather than passthrough.
 * 2. **It matches spellings, not types.** Its `trigger` is a literal substring test over
 *    rendered output, so `BatchUpdateException` never matched a rule triggered on
 *    `"SQLException"` - which is how DASE-3667 leaked twice. Here the real class and the whole
 *    cause chain are available.
 * 3. **It cannot tell a format string from an argument.** A [ParameterizedMessage]'s format
 *    string is a literal from Connect's own source and carries no data; only the arguments do.
 *    So `"Write of {} records failed, remainingRetries={}"` survives verbatim while its
 *    arguments are scrubbed. That is what keeps a redacted ERROR troubleshootable, and it is
 *    not expressible as a regex over the already-formatted message.
 *
 * Redaction is scoped by level so INFO and WARN stay readable:
 *
 * ```
 * Rewrite:
 *   name: RedactingAppender
 *   AppenderRef:
 *     ref: JsonConsole
 *   PiiRedactionPolicy:
 *     redactAtOrAbove: ERROR
 *     aggressiveAtOrAbove: WARN
 *     aggressiveLoggers: "io.confluent.connect.jdbc,org.apache.kafka.connect.runtime.errors"
 * ```
 *
 * `aggressiveLoggers` exists because `JdbcSinkTask` logs the driver's `SQLException` at WARN on
 * every retry, so those loggers need a lower threshold than the rest of the fleet. Because the
 * level decision lives here, the log4j2 config needs no `ThresholdFilter`s and no duplicated
 * `AppenderRef`s - `Root` points at this one appender.
 *
 * Structural fields (timestamp, level, loggerName, thread, `contextMap.connector.context`) are
 * added by the layout after this policy runs and are untouched, so which connector and task
 * failed is always visible.
 *
 * @see panics for the health signal this class exposes.
 */
@Plugin(
    name = "PiiRedactionPolicy",
    category = Core.CATEGORY_NAME,
    elementType = "rewritePolicy",
    printObject = true,
)
class PiiRedactionPolicy private constructor(
    private val redactAtOrAbove: Level,
    private val aggressiveAtOrAbove: Level,
    private val aggressiveLoggers: List<String>,
    private val keepStackFrames: Boolean,
) : RewritePolicy {

    companion object {
        private const val REDACTED = "[REDACTED]"

        /** A cause chain may be arbitrarily deep; cap it so logging cannot become the bottleneck. */
        private const val MAX_CAUSE_DEPTH = 20

        /** MDC key Connect populates with `"[<connector>|task-<n>] "`. */
        private const val CONNECTOR_CONTEXT = "connector.context"

        private val panicCount = AtomicLong()

        /**
         * Number of events that hit the fail-closed fallback in [rewrite].
         *
         * Non-zero means this class is broken: nothing is leaking, but detail is being discarded
         * fleet-wide. Alarm on it. This counter exists because an `initCause` bug in an early
         * version routed every event carrying a throwable through the fallback, and the only
         * external symptom was slightly emptier logs.
         */
        fun panics(): Long = panicCount.get()

        @JvmStatic
        @PluginFactory
        fun createPolicy(
            @PluginAttribute(value = "redactAtOrAbove", defaultString = "ERROR") redactAtOrAbove: String?,
            @PluginAttribute(value = "aggressiveAtOrAbove", defaultString = "WARN") aggressiveAtOrAbove: String?,
            @PluginAttribute("aggressiveLoggers") aggressiveLoggers: String?,
            @PluginAttribute(value = "keepStackFrames", defaultBoolean = true) keepStackFrames: Boolean,
        ): PiiRedactionPolicy =
            PiiRedactionPolicy(
                // Level.toLevel falls back to its second argument on an unparseable name, so a
                // typo in the config tightens redaction rather than disabling it.
                redactAtOrAbove = Level.toLevel(redactAtOrAbove, Level.ERROR),
                aggressiveAtOrAbove = Level.toLevel(aggressiveAtOrAbove, Level.WARN),
                aggressiveLoggers = aggressiveLoggers
                    ?.split(",")
                    ?.map { it.trim() }
                    ?.filter { it.isNotEmpty() }
                    ?: emptyList(),
                keepStackFrames = keepStackFrames,
            )

        /**
         * Connector name from the MDC, for per-connector policy.
         *
         * Not usable from a log4j2 `ThreadContextMapFilter`, which matches values exactly while
         * this value embeds the task index - but trivial to read here.
         */
        fun connectorName(event: LogEvent): String? {
            val ctx = event.contextData?.getValue<String>(CONNECTOR_CONTEXT) ?: return null
            val start = ctx.indexOf('[')
            if (start < 0) return null
            val end = ctx.indexOf('|', start + 1).takeIf { it >= 0 }
                ?: ctx.indexOf(']', start + 1).takeIf { it >= 0 }
                ?: return null
            return ctx.substring(start + 1, end)
        }
    }

    override fun rewrite(event: LogEvent?): LogEvent? {
        if (event == null) return null
        return try {
            if (shouldRedact(event)) redact(event) else event
        } catch (oops: Throwable) {
            // A bug in this class must never become a PII leak.
            panicRedact(event, oops)
        }
    }

    /** Default deny: an event we cannot classify is redacted. */
    private fun shouldRedact(event: LogEvent): Boolean {
        val level = event.level ?: return true
        val logger = event.loggerName
        if (logger != null) {
            for (prefix in aggressiveLoggers) {
                if (logger.startsWith(prefix)) return level.isMoreSpecificThan(aggressiveAtOrAbove)
            }
        }
        return level.isMoreSpecificThan(redactAtOrAbove)
    }

    private fun redact(event: LogEvent): LogEvent =
        Log4jLogEvent.Builder(event)
            .setMessage(redactMessage(event.message))
            .setThrown(sanitize(event.thrown, identitySet(), 0))
            .build()
    // Deliberately no setThrownProxy: it is a no-op stub in log4j-core 2.25 and the
    // copy-constructor does not carry the source event's proxy, so getThrownProxy() is rebuilt
    // lazily from the sanitised throwable above.

    /**
     * Keeps a [ParameterizedMessage]'s format string and scrubs its arguments.
     *
     * Anything else - notably [SimpleMessage], which is what string concatenation at the call
     * site produces - is replaced wholesale, because the data is already baked into the text and
     * there is no structure left to exploit.
     */
    @Suppress("DEPRECATION") // Message.format is deprecated but is the only accessor for the pattern.
    private fun redactMessage(message: Message?): Message {
        if (message !is ParameterizedMessage) return SimpleMessage(REDACTED)
        val format = message.format ?: return SimpleMessage(REDACTED)
        val params = message.parameters
        if (params.isNullOrEmpty()) return SimpleMessage(format)
        val scrubbed: Array<Any?> = Array(params.size) { scrubParam(params[it]) }
        // The array MUST be spread. ParameterizedMessage overloads both (String, Object...) and
        // (String, Object); without the spread operator Kotlin binds to the latter, passing the
        // whole array as argument one, which leaves the remaining {} placeholders unsubstituted.
        return ParameterizedMessage(format, *scrubbed)
    }

    /**
     * Allowlist. Numbers and booleans are the counts, offsets, partitions and retry budgets that
     * make a Connect log readable; everything else - String, SinkRecord, Struct, ByteArray - is
     * assumed to carry data.
     *
     * Residual risk, stated plainly: a numeric value that is itself sensitive (a salary, an
     * employee id) would survive if a call site passed it as a log argument. No Connect framework
     * logger does this, but a third-party plugin could. Redact all parameters if that is not an
     * acceptable residual.
     */
    private fun scrubParam(param: Any?): Any? = when (param) {
        null -> null
        is Number, is Boolean -> param
        else -> REDACTED
    }

    /**
     * Rebuilds the cause chain preserving class names and stack frames, dropping every message.
     *
     * Guarded by identity and depth: a self-referencing cause chain is legal Java and would
     * otherwise hang the logging thread.
     *
     * Does not reflectively reconstruct the original exception type, which is what the Confluent
     * redactor does. That runs arbitrary driver constructors inside the logging path, and many
     * exception classes have no `(String)` constructor. Carrying the class name as text is
     * strictly safer and renders just as usefully.
     */
    private fun sanitize(t: Throwable?, seen: MutableSet<Throwable>, depth: Int): Throwable? {
        if (t == null || depth > MAX_CAUSE_DEPTH || !seen.add(t)) return null

        // The cause must be passed to the constructor, never via initCause(): Throwable's 4-arg
        // constructor *sets* the cause field even when given null, after which initCause() throws
        // IllegalStateException("Can't overwrite cause").
        val cause = sanitize(t.cause, seen, depth + 1)
        val safe = RedactedThrowable("${t.javaClass.name}: $REDACTED", cause)
        if (keepStackFrames) safe.stackTrace = t.stackTrace
        t.suppressed.forEach { suppressed ->
            sanitize(suppressed, seen, depth + 1)?.let(safe::addSuppressed)
        }
        return safe
    }

    /** Last resort. Must not throw. Keeps routing metadata, discards all content. */
    private fun panicRedact(event: LogEvent, cause: Throwable): LogEvent {
        val n = panicCount.incrementAndGet()
        // Loud for the first occurrence then on powers of two, so a persistent fault stays
        // visible without flooding. Reported via StatusLogger, not a logger - going back through
        // an appender would recurse into this policy.
        if (java.lang.Long.bitCount(n) == 1) {
            StatusLogger.getLogger().error(
                "PiiRedactionPolicy failed and fell back to full redaction (count=$n). " +
                    "Logs are safe but degraded; this is a bug.",
                cause,
            )
        }
        return try {
            Log4jLogEvent.Builder(event).setMessage(SimpleMessage(REDACTED)).setThrown(null).build()
        } catch (stillBroken: Throwable) {
            Log4jLogEvent.newBuilder()
                .setLevel(Level.ERROR)
                .setLoggerName(PiiRedactionPolicy::class.java.name)
                .setMessage(SimpleMessage(REDACTED))
                .build()
        }
    }

    private fun identitySet(): MutableSet<Throwable> =
        Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())

    override fun toString(): String =
        "PiiRedactionPolicy{redactAtOrAbove=$redactAtOrAbove, " +
            "aggressiveAtOrAbove=$aggressiveAtOrAbove, aggressiveLoggers=$aggressiveLoggers}"

    /**
     * Carries a class name and nothing else.
     *
     * Both trailing flags must stay `true`: `enableSuppression = false` silently makes
     * [addSuppressed] a no-op, and `writableStackTrace = false` silently makes [setStackTrace]
     * a no-op.
     */
    internal class RedactedThrowable(message: String, cause: Throwable?) :
        RuntimeException(message, cause, true, true)
}
