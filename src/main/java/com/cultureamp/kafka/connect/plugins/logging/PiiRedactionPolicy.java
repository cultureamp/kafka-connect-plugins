package com.cultureamp.kafka.connect.plugins.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.rewrite.RewritePolicy;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.status.StatusLogger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Strips exception messages from Kafka Connect logs when a JDBC driver exception is involved.
 *
 * <p>Replaces {@code io.confluent.log4j2.redactor.RedactorPolicy}, whose regex rules gated on a
 * literal {@code trigger} substring and so failed open twice (DASE-3667): {@code
 * BatchUpdateException} never matched a rule triggered on {@code "SQLException"}. The stopgap for
 * that (kafka-ops#5134) redacted every log message fleet-wide, roughly 15k INFO events per 15
 * minutes all reading {@code [REDACTED]}.
 *
 * <h2>What it does</h2>
 *
 * <pre>
 *   chain contains a SQLException  -&gt; drop every exception message in the chain
 *   logger is in untrustedLoggers  -&gt; drop the log message
 *   otherwise                      -&gt; return the event untouched
 * </pre>
 *
 * Exception class names and stack frames are always kept: a {@code StackTraceElement} is declaring
 * class, method, file and line, read from the class file constant pool, so it cannot carry runtime
 * data. Neither can a class name. That is what leaves a redacted error diagnosable.
 *
 * <h2>Why this shape</h2>
 *
 * Measured over 13 days and 3,728 production error events:
 *
 * <pre>
 *   log message contains payload        0
 *   exception message contains payload  37+
 * </pre>
 *
 * The payload lives exclusively in exception messages, so log messages are left completely alone.
 * An earlier version of this class also rewrote them - preserving format strings, scrubbing
 * arguments, allowlisting identifier types - and every bit of that was unnecessary.
 *
 * <p>Three things drive the remaining design, each verified rather than assumed:
 *
 * <ul>
 *   <li><b>Gate on the chain, not the outer type.</b> {@code JdbcSinkTask.getAllMessagesException}
 *       walks the chain, appends each exception's {@code toString()} and builds a <em>new</em>
 *       {@link SQLException} whose message is that concatenation (confirmed from bytecode).
 *       Wrappers copy it again, because {@code Throwable(Throwable cause)} sets
 *       {@code message = cause.toString()}. Real events show the payload in a
 *       {@code RetriableException}'s own message, and separately only in the cause while the outer
 *       {@code ConnectException} message stays clean. On 12 of 36 the outermost throwable was a
 *       bare {@code java.lang.Throwable}.
 *   <li><b>{@code SQLException}, not {@code BatchUpdateException}.</b> The latter is one leaf; auth
 *       failures and constraint violations never batch, and the synthetic exception above is a
 *       plain {@code SQLException}. Every JDBC driver exception is a {@code SQLException} by the
 *       {@code java.sql} contract, so one check covers {@code BatchUpdateException},
 *       {@code PSQLException} and {@code RedshiftException} together.
 *   <li><b>No level thresholds.</b> An earlier version redacted at ERROR and dropped to WARN for
 *       chosen loggers as a proxy for "where might PII be". The chain check is the real signal,
 *       which makes the proxy redundant and closes a gap it had: {@code JdbcSinkTask} advises
 *       enabling DEBUG for exception detail, which used to produce unredacted output.
 * </ul>
 *
 * <p>{@code untrustedLoggers} exists for what a type check structurally cannot see: {@code
 * LogReporter} assembles the failed record into its log message by string concatenation, with no
 * exception involved. That is the {@code errors.log.include.messages} channel. It has never fired
 * in production, so this is precautionary.
 *
 * <p>Accepted residual: a call site that concatenates row data into a log message, or a non-JDBC
 * plugin exception carrying row data, would not be caught. Neither appears in 3,728 production
 * events, but the S3 and Lambda connectors were never audited.
 *
 * <h2>Deployment</h2>
 *
 * <p>Java, not Kotlin, and shipped as its own jar. log4j2 resolves plugins at JVM startup via the
 * system classloader from the Connect <em>worker</em> classpath, where {@code kafka-run-class} puts
 * only {@code share/java/kafka} and {@code share/java/confluent-telemetry}. {@code kotlin-stdlib}
 * is on neither, so a Kotlin policy dies with {@code NoClassDefFoundError} - after which log4j2
 * builds the Rewrite appender with <em>no policy</em> and logs everything unredacted.
 *
 * <pre>
 *   Rewrite:
 *     name: RedactingAppender
 *     AppenderRef:
 *       ref: JsonConsole
 *     PiiRedactionPolicy:
 *       untrustedLoggers: "org.apache.kafka.connect.runtime.errors"
 * </pre>
 *
 * @see #panics() the health signal to alarm on
 */
@Plugin(
        name = "PiiRedactionPolicy",
        category = Core.CATEGORY_NAME,
        elementType = "rewritePolicy",
        printObject = true)
public final class PiiRedactionPolicy implements RewritePolicy {

    private static final String REDACTED = "[REDACTED]";

    /** A cause chain may be arbitrarily deep, or cyclic; cap the walk. */
    private static final int MAX_CAUSE_DEPTH = 20;

    private static final AtomicLong PANIC_COUNT = new AtomicLong();

    private final List<String> untrustedLoggers;

    private PiiRedactionPolicy(final List<String> untrustedLoggers) {
        this.untrustedLoggers = untrustedLoggers;
    }

    @PluginFactory
    public static PiiRedactionPolicy createPolicy(
            @PluginAttribute(value = "untrustedLoggers",
                    defaultString = "org.apache.kafka.connect.runtime.errors") final String untrustedLoggers) {
        return new PiiRedactionPolicy(splitCsv(untrustedLoggers));
    }

    /**
     * Number of events that hit the fail-closed fallback in {@link #rewrite}.
     *
     * <p>Non-zero means this class is broken: nothing is leaking, but detail is being discarded.
     * Alarm on it. It exists because an {@code initCause} bug in an early version routed every
     * event carrying a throwable through the fallback, and the only external symptom was slightly
     * emptier logs.
     */
    public static long panics() {
        return PANIC_COUNT.get();
    }

    @Override
    public LogEvent rewrite(final LogEvent event) {
        if (event == null) {
            return null;
        }
        try {
            final boolean dropLogMessage = isUntrustedLogger(event.getLoggerName());
            final Throwable thrown = event.getThrown();
            final boolean dropExceptionMessages = chainHasSqlException(thrown);

            if (!dropLogMessage && !dropExceptionMessages) {
                return event;
            }

            final Log4jLogEvent.Builder rewritten = new Log4jLogEvent.Builder(event);
            if (dropLogMessage) {
                rewritten.setMessage(new SimpleMessage(REDACTED));
            }
            if (dropExceptionMessages) {
                rewritten.setThrown(sanitize(thrown, identitySet(), 0));
            }
            return rewritten.build();
            // No setThrownProxy: it is a no-op stub in log4j-core 2.25 and the copy-constructor
            // does not carry the source event's proxy, so getThrownProxy() is rebuilt from the
            // sanitised throwable.
        } catch (final Throwable oops) {
            // A bug in this class must never become a PII leak.
            return panicRedact(event, oops);
        }
    }

    private boolean isUntrustedLogger(final String logger) {
        if (logger == null) {
            return false;
        }
        for (final String prefix : untrustedLoggers) {
            if (logger.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * True if any throwable in the cause or suppressed chain is a {@link SQLException}.
     *
     * <p>Fails closed: a hostile accessor, a cycle, or a chain deeper than
     * {@link #MAX_CAUSE_DEPTH} all return true rather than assume the chain is clean.
     */
    private static boolean chainHasSqlException(final Throwable thrown) {
        final Set<Throwable> seen = identitySet();
        Throwable current = thrown;
        int depth = 0;
        while (current != null) {
            if (depth++ > MAX_CAUSE_DEPTH || !seen.add(current)) {
                return true;
            }
            if (current instanceof SQLException) {
                return true;
            }
            try {
                for (final Throwable suppressed : current.getSuppressed()) {
                    if (suppressed instanceof SQLException) {
                        return true;
                    }
                }
                current = current.getCause();
            } catch (final Throwable hostileAccessor) {
                return true;
            }
        }
        return false;
    }

    /**
     * Rebuilds the cause chain preserving class names and stack frames, dropping every message.
     *
     * <p>Each accessor is guarded independently, because {@code getCause}, {@code getMessage} and
     * {@code getStackTrace} are all overridable and a badly-behaved exception class from any plugin
     * can throw from any of them. Without per-field guards one such accessor fails the whole event
     * and costs the entire class chain. {@code getClass} and {@code getSuppressed} are final in
     * {@code Throwable} and cannot misbehave.
     *
     * <p>Does not reflectively reconstruct the original type, which is what the Confluent redactor
     * does: that runs arbitrary driver constructors inside the logging path, and many exception
     * classes have no {@code (String)} constructor.
     */
    private Throwable sanitize(final Throwable t, final Set<Throwable> seen, final int depth) {
        if (t == null || depth > MAX_CAUSE_DEPTH || !seen.add(t)) {
            return null;
        }
        // The cause must be passed to the constructor, never via initCause(): Throwable's 4-arg
        // constructor *sets* the cause field even when given null, after which initCause() throws
        // IllegalStateException("Can't overwrite cause").
        Throwable cause = null;
        try {
            cause = sanitize(t.getCause(), seen, depth + 1);
        } catch (final Throwable hostileGetCause) {
            cause = null;
        }

        final Throwable safe = new RedactedThrowable(t.getClass().getName() + ": " + REDACTED, cause);

        try {
            safe.setStackTrace(t.getStackTrace());
        } catch (final Throwable hostileGetStackTrace) {
            // Leave the frames RedactedThrowable was constructed with.
        }

        for (final Throwable suppressed : t.getSuppressed()) {
            try {
                final Throwable s = sanitize(suppressed, seen, depth + 1);
                if (s != null) {
                    safe.addSuppressed(s);
                }
            } catch (final Throwable hostileSuppressed) {
                // Drop this suppressed entry only.
            }
        }
        return safe;
    }

    /** Last resort. Must not throw. Keeps routing metadata, discards all content. */
    private LogEvent panicRedact(final LogEvent event, final Throwable cause) {
        final long n = PANIC_COUNT.incrementAndGet();
        // Loud on the first occurrence then on powers of two, so a persistent fault stays visible
        // without flooding. Via StatusLogger, not a logger - an appender would recurse into here.
        if (Long.bitCount(n) == 1) {
            StatusLogger.getLogger()
                    .error(
                            "PiiRedactionPolicy failed and fell back to full redaction (count={}). "
                                    + "Logs are safe but degraded; this is a bug.",
                            n,
                            cause);
        }
        try {
            return new Log4jLogEvent.Builder(event)
                    .setMessage(new SimpleMessage(REDACTED))
                    .setThrown(null)
                    .build();
        } catch (final Throwable stillBroken) {
            return Log4jLogEvent.newBuilder()
                    .setLevel(Level.ERROR)
                    .setLoggerName(PiiRedactionPolicy.class.getName())
                    .setMessage(new SimpleMessage(REDACTED))
                    .build();
        }
    }

    private static Set<Throwable> identitySet() {
        return Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
    }

    private static List<String> splitCsv(final String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return Collections.emptyList();
        }
        final List<String> out = new ArrayList<>();
        for (final String part : csv.split(",")) {
            final String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return Collections.unmodifiableList(out);
    }

    @Override
    public String toString() {
        return "PiiRedactionPolicy{untrustedLoggers=" + untrustedLoggers + '}';
    }

    /**
     * Carries a class name and nothing else.
     *
     * <p>Both trailing flags must stay {@code true}: {@code enableSuppression = false} silently
     * makes {@link Throwable#addSuppressed} a no-op, and {@code writableStackTrace = false}
     * silently makes {@link Throwable#setStackTrace} a no-op.
     */
    static final class RedactedThrowable extends RuntimeException {
        private static final long serialVersionUID = 1L;

        RedactedThrowable(final String message, final Throwable cause) {
            super(message, cause, true, true);
        }
    }
}
