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
 *   otherwise                      -&gt; return the event untouched
 * </pre>
 *
 * As shipped that is the whole of it - log messages are never touched, whatever the logger. {@code
 * untrustedLoggers} can opt a named logger into losing its message too, and defaults to empty.
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
 * <h2>untrustedLoggers, and why it ships empty</h2>
 *
 * <p>{@code untrustedLoggers} is an opt-in escape hatch for what a type check structurally cannot
 * see: a logger that concatenates record data into its message with no exception involved. Naming a
 * logger prefix there drops that logger's log message <em>and</em> its exception messages, on the
 * grounds that a call site known to handle record data should not be filtered by exception type.
 *
 * <p>It defaults to empty, so out of the box nothing is matched and the {@code SQLException} gate is
 * the only thing that redacts. The known candidate was {@code LogReporter}, the {@code
 * errors.log.include.messages} channel, and it was not worth enabling by default:
 *
 * <ul>
 *   <li>It never fired in production, and for a <em>sink</em> connector it never could:
 *       {@code ProcessingContext.toString} appends the full record only on the source-record branch.
 *       For a consumed record it appends {@code topic/partition/offset/timestamp} and nothing else -
 *       no key, no value. That is consistent with 0 of 3,728 events having a payload in the message.
 *   <li>The payload risk at that call site is the throwable, not the message: {@code
 *       LogReporter.report} passes {@code context.error()} alongside it, and for the JDBC sink that
 *       error is the {@code SQLException} chain - already covered by the gate.
 *   <li>The obvious prefix to configure, the {@code org.apache.kafka.connect.runtime.errors} package,
 *       catches four unrelated call sites in it. {@code DeadLetterQueueReporter} and {@code
 *       WorkerErrantRecordReporter} log static strings plus a Kafka producer exception; blanking
 *       those loses real diagnostics ("which topic could I not write the DLQ record to") to protect
 *       nothing. Configure the single class, not the package, if you ever need this.
 * </ul>
 *
 * <p>Accepted residual, therefore: a call site that concatenates row data into a log message is not
 * caught, and neither is a non-JDBC plugin exception carrying row data. Specifically, a source
 * connector with {@code errors.log.include.messages=true} would log the full record via {@code
 * LogReporter}, and a converter failure carries the record in a Jackson {@code JsonParseException}
 * message. Neither appears in 3,728 production events, all of which are sink connectors, but the S3
 * and Lambda connectors were never audited. If either shows up, name that logger here.
 *
 * <h2>Deployment</h2>
 *
 * <p>Java, not Kotlin, and shipped as its own jar. log4j2 resolves plugins at JVM startup via the
 * system classloader from the Connect <em>worker</em> classpath, where {@code kafka-run-class} puts
 * only {@code share/java/kafka} and {@code share/java/confluent-telemetry}. {@code kotlin-stdlib}
 * is on neither, so a Kotlin policy dies with {@code NoClassDefFoundError} - after which log4j2
 * builds the Rewrite appender with <em>no policy</em> and logs everything unredacted.
 *
 * <p>No attributes are required. In YAML the empty element has to be an explicit empty mapping - a
 * bare {@code PiiRedactionPolicy:} parses as a null value, not as a node:
 *
 * <pre>
 *   Rewrite:
 *     name: RedactingAppender
 *     AppenderRef:
 *       ref: JsonConsole
 *     PiiRedactionPolicy: {}
 *
 *   # only if a log message ever turns out to leak; a single class, not a package:
 *     PiiRedactionPolicy:
 *       untrustedLoggers: "org.apache.kafka.connect.runtime.errors.LogReporter"
 * </pre>
 *
 * <p>Check the worker's status output once after changing this. A policy log4j2 cannot resolve does
 * not fail the appender - it builds Rewrite with <em>no policy</em> and logs everything unredacted.
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

    /**
     * A cause chain may be arbitrarily deep, or cyclic; cap the walk. Hitting the cap fails closed:
     * the chain is redacted and truncated here, so a pathological chain costs detail, not safety.
     * Measured boundary: 16 nested throwables pass through untouched, 22 trip the cap.
     */
    private static final int MAX_CAUSE_DEPTH = 20;

    private static final AtomicLong PANIC_COUNT = new AtomicLong();

    private final List<String> untrustedLoggers;

    private PiiRedactionPolicy(final List<String> untrustedLoggers) {
        this.untrustedLoggers = untrustedLoggers;
    }

    /**
     * @param untrustedLoggers comma-separated logger-name prefixes whose log messages and exception
     *     messages are both dropped unconditionally. Empty by default: see the class docs for why,
     *     and prefer naming a single class over a package if you set it.
     */
    @PluginFactory
    public static PiiRedactionPolicy createPolicy(
            @PluginAttribute(value = "untrustedLoggers", defaultString = "") final String untrustedLoggers) {
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
            // Short-circuited on the empty default, so the shipped configuration does not even look
            // at the logger name.
            final boolean dropLogMessage =
                    !untrustedLoggers.isEmpty() && isUntrustedLogger(event.getLoggerName());
            final Throwable thrown = event.getThrown();

            // ONE walk over the exception graph: it builds the sanitised copy and decides whether
            // the chain needs redacting at all. Skipped when there is no throwable, which is almost
            // every event - a worker logs on the order of 15k INFO events per 15 minutes and next
            // to none of them carry one - so the common path stays allocation-free.
            Throwable safe = null;
            boolean dropExceptionMessages = false;
            if (thrown != null) {
                final ChainScan scan = new ChainScan();
                safe = sanitize(thrown, scan, identitySet(), 0);
                dropExceptionMessages = scan.mustRedact;
            }

            if (!dropLogMessage && !dropExceptionMessages) {
                return event;
            }

            final Log4jLogEvent.Builder rewritten = new Log4jLogEvent.Builder(event);
            if (dropLogMessage) {
                rewritten.setMessage(new SimpleMessage(REDACTED));
            }
            // Unconditional: an untrusted logger drops exception messages too, and if the chain is
            // clean and the logger trusted we returned the event untouched above.
            rewritten.setThrown(safe);
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
     * Rebuilds the cause chain preserving class names and stack frames, dropping every message, and
     * records in {@code scan} whether the chain needs redacting at all.
     *
     * <p>Detection is fused into this walk deliberately. It used to be a separate
     * {@code chainHasSqlException} pass, and the two drifted: detection checked suppressed entries
     * with a shallow {@code instanceof} while this method recursed into them, so a
     * {@link SQLException} nested <em>below</em> a suppressed exception was sanitised correctly but
     * never triggered redaction and the event went out with the payload intact. The JDK offers no
     * traversal of the cause-and-suppressed graph ({@code Throwable} exposes only the two
     * accessors), and the third-party helpers that look like it - {@code
     * ExceptionUtils.throwableOfType}, {@code Throwables.getCausalChain} - walk the cause chain only
     * and trust the accessors. So the walk has to be hand-written; making it the only walk is what
     * keeps detection and sanitisation from disagreeing again.
     *
     * <p>Each accessor is guarded independently, because {@code getCause} and {@code getStackTrace}
     * are both overridable and a badly-behaved exception class from any plugin can throw from any of
     * them. Without per-field guards one such accessor fails the whole event and costs the entire
     * class chain. {@code getClass} and {@code getSuppressed} are final in {@code Throwable} and
     * cannot misbehave. Anything that stops the walk seeing a subtree sets
     * {@link ChainScan#mustRedact}: a chain that cannot be proved clean is treated as dirty.
     *
     * <p>{@code SQLException.getNextException()} is not walked, and does not need to be. It is not
     * reachable from the sanitised copy - {@link RedactedThrowable} carries a class name and a cause,
     * nothing else - and {@link Throwable#printStackTrace} never renders it, so a next-exception
     * message cannot reach an appender. {@code JdbcSinkTask} is the thing that walks it, which is
     * how the payload ends up concatenated into a synthetic {@code SQLException} message instead.
     *
     * <p>Does not reflectively reconstruct the original type, which is what the Confluent redactor
     * does: that runs arbitrary driver constructors inside the logging path, and many exception
     * classes have no {@code (String)} constructor.
     */
    private Throwable sanitize(
            final Throwable t, final ChainScan scan, final Set<Throwable> seen, final int depth) {
        if (t == null) {
            return null;
        }
        if (depth > MAX_CAUSE_DEPTH || !seen.add(t)) {
            // Too deep, cyclic, or reached twice: nothing below here can be proved clean.
            scan.mustRedact = true;
            return null;
        }
        if (t instanceof SQLException) {
            scan.mustRedact = true;
        }
        // The cause must be passed to the constructor, never via initCause(): Throwable's 4-arg
        // constructor *sets* the cause field even when given null, after which initCause() throws
        // IllegalStateException("Can't overwrite cause").
        Throwable cause = null;
        try {
            cause = sanitize(t.getCause(), scan, seen, depth + 1);
        } catch (final Throwable hostileGetCause) {
            scan.mustRedact = true;
        }

        final Throwable safe = new RedactedThrowable(t.getClass().getName() + ": " + REDACTED, cause);

        try {
            safe.setStackTrace(t.getStackTrace());
        } catch (final Throwable hostileGetStackTrace) {
            // Leave the frames RedactedThrowable was constructed with. Frames cannot carry runtime
            // data, so this costs detail only - it says nothing about what is in the chain.
        }

        for (final Throwable suppressed : t.getSuppressed()) {
            try {
                final Throwable s = sanitize(suppressed, scan, seen, depth + 1);
                if (s != null) {
                    safe.addSuppressed(s);
                }
            } catch (final Throwable hostileSuppressed) {
                // Drop this suppressed entry - and we did not get to see inside it.
                scan.mustRedact = true;
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
     * Out-parameter for {@link #sanitize}: {@code true} once the walk has seen a {@link SQLException},
     * or has hit something that stops it proving there is not one.
     */
    private static final class ChainScan {
        private boolean mustRedact;
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
