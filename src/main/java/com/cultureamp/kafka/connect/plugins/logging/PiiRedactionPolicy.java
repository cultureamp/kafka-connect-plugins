package com.cultureamp.kafka.connect.plugins.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.rewrite.RewritePolicy;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Structural PII redaction for Kafka Connect logs.
 *
 * <p>A log4j2 {@link RewritePolicy} that decides what to redact from the {@link LogEvent} object
 * graph rather than by pattern-matching rendered text. Intended to replace {@code
 * io.confluent.log4j2.redactor.RedactorPolicy}, whose regex rule file has three properties this
 * does not:
 *
 * <ol>
 *   <li><b>It fails open.</b> {@code StringRedactorEngine} catches a rules-compile error, logs one
 *       line and installs an <em>empty</em> redactor, so a typo silently disables redaction
 *       entirely. Here the default is to redact, and {@link #rewrite} catches its own bugs and
 *       degrades to full redaction rather than passthrough.
 *   <li><b>It matches spellings, not types.</b> Its {@code trigger} is a literal substring test
 *       over rendered text, so {@code BatchUpdateException} never matched a rule triggered on
 *       {@code "SQLException"} - which is how DASE-3667 leaked twice. Here the real class and the
 *       whole cause chain are available, and {@link #sanitize} does not branch on either: every
 *       throwable is treated identically, so no exception any connector can throw changes the
 *       outcome.
 *   <li><b>It cannot tell a format string from an argument.</b> A parameterised message's format
 *       string is a literal from Connect's source and carries no data; only the arguments do. So
 *       {@code "Write of {} records failed, remainingRetries={}"} survives verbatim while its
 *       arguments are scrubbed. That is what keeps a redacted ERROR troubleshootable, and it is
 *       not expressible as a regex over the formatted message.
 * </ol>
 *
 * <p><b>This is deliberately Java, not Kotlin, and ships as its own jar.</b> log4j2 plugins are
 * loaded at JVM startup by the system classloader from the Connect <em>worker</em> classpath
 * ({@code kafka-run-class} adds only {@code share/java/kafka} and {@code
 * share/java/confluent-telemetry}). {@code kotlin-stdlib} is not on that classpath, so a Kotlin
 * policy dies with {@code NoClassDefFoundError: kotlin/jvm/internal/Intrinsics}; log4j2 then
 * builds the Rewrite appender with no policy at all and passes every event through unredacted.
 * Java needs nothing beyond log4j-core, which is already there. The SMTs in this repo are
 * unaffected - they are loaded later, from {@code plugin.path}, by a connector classloader.
 *
 * <p>Redaction is scoped by level so INFO and WARN stay readable:
 *
 * <pre>
 *   Rewrite:
 *     name: RedactingAppender
 *     AppenderRef:
 *       ref: JsonConsole
 *     PiiRedactionPolicy:
 *       redactAtOrAbove: ERROR
 *       aggressiveAtOrAbove: WARN
 *       aggressiveLoggers: "io.confluent.connect.jdbc,org.apache.kafka.connect.runtime.errors"
 * </pre>
 *
 * <p>{@code aggressiveLoggers} exists because {@code JdbcSinkTask} logs the driver's {@code
 * SQLException} at WARN on every retry, and {@code LogReporter} emits the record dump when
 * {@code errors.log.include.messages} is true. Because the level decision lives here, the log4j2
 * config needs no {@code ThresholdFilter}s and no duplicated {@code AppenderRef}s.
 *
 * <p>Structural fields (timestamp, level, loggerName, thread, {@code
 * contextMap.connector.context}) are added by the layout after this policy runs and are
 * untouched, so which connector and task failed is always visible.
 *
 * @see #panics() for the health signal this class exposes.
 */
@Plugin(
        name = "PiiRedactionPolicy",
        category = Core.CATEGORY_NAME,
        elementType = "rewritePolicy",
        printObject = true)
public final class PiiRedactionPolicy implements RewritePolicy {

    private static final String REDACTED = "[REDACTED]";

    /** A cause chain may be arbitrarily deep; cap it so logging cannot become the bottleneck. */
    private static final int MAX_CAUSE_DEPTH = 20;

    /** MDC key Connect populates with {@code "[<connector>|task-<n>] "}. */
    private static final String CONNECTOR_CONTEXT = "connector.context";

    private static final AtomicLong PANIC_COUNT = new AtomicLong();

    private final Level redactAtOrAbove;
    private final Level aggressiveAtOrAbove;
    private final List<String> aggressiveLoggers;
    private final boolean keepStackFrames;

    private PiiRedactionPolicy(
            final Level redactAtOrAbove,
            final Level aggressiveAtOrAbove,
            final List<String> aggressiveLoggers,
            final boolean keepStackFrames) {
        this.redactAtOrAbove = redactAtOrAbove;
        this.aggressiveAtOrAbove = aggressiveAtOrAbove;
        this.aggressiveLoggers = aggressiveLoggers;
        this.keepStackFrames = keepStackFrames;
    }

    @PluginFactory
    public static PiiRedactionPolicy createPolicy(
            @PluginAttribute(value = "redactAtOrAbove", defaultString = "ERROR") final String redactAtOrAbove,
            @PluginAttribute(value = "aggressiveAtOrAbove", defaultString = "WARN") final String aggressiveAtOrAbove,
            @PluginAttribute("aggressiveLoggers") final String aggressiveLoggers,
            @PluginAttribute(value = "keepStackFrames", defaultBoolean = true) final boolean keepStackFrames) {

        return new PiiRedactionPolicy(
                // Level.toLevel falls back to its second argument on an unparseable name, so a
                // typo in the config tightens redaction rather than disabling it.
                Level.toLevel(redactAtOrAbove, Level.ERROR),
                Level.toLevel(aggressiveAtOrAbove, Level.WARN),
                splitCsv(aggressiveLoggers),
                keepStackFrames);
    }

    /**
     * Number of events that hit the fail-closed fallback in {@link #rewrite}.
     *
     * <p>Non-zero means this class is broken: nothing is leaking, but detail is being discarded
     * fleet-wide. Alarm on it. This counter exists because an {@code initCause} bug in an early
     * version routed every event carrying a throwable through the fallback, and the only external
     * symptom was slightly emptier logs.
     */
    public static long panics() {
        return PANIC_COUNT.get();
    }

    /**
     * Connector name from the MDC, for per-connector policy.
     *
     * <p>Not usable from a log4j2 {@code ThreadContextMapFilter}, which matches values exactly
     * while this value embeds the task index - but trivial to read here.
     */
    public static String connectorName(final LogEvent event) {
        final ReadOnlyStringMap contextData = event == null ? null : event.getContextData();
        final String ctx = contextData == null ? null : contextData.getValue(CONNECTOR_CONTEXT);
        if (ctx == null) {
            return null;
        }
        final int start = ctx.indexOf('[');
        if (start < 0) {
            return null;
        }
        int end = ctx.indexOf('|', start + 1);
        if (end < 0) {
            end = ctx.indexOf(']', start + 1);
        }
        return end < 0 ? null : ctx.substring(start + 1, end);
    }

    @Override
    public LogEvent rewrite(final LogEvent event) {
        if (event == null) {
            return null;
        }
        try {
            return shouldRedact(event) ? redact(event) : event;
        } catch (final Throwable oops) {
            // A bug in this class must never become a PII leak.
            return panicRedact(event, oops);
        }
    }

    /** Default deny: an event we cannot classify is redacted. */
    private boolean shouldRedact(final LogEvent event) {
        final Level level = event.getLevel();
        if (level == null) {
            return true;
        }
        final String logger = event.getLoggerName();
        if (logger != null) {
            for (final String prefix : aggressiveLoggers) {
                if (logger.startsWith(prefix)) {
                    return level.isMoreSpecificThan(aggressiveAtOrAbove);
                }
            }
        }
        return level.isMoreSpecificThan(redactAtOrAbove);
    }

    private LogEvent redact(final LogEvent event) {
        return new Log4jLogEvent.Builder(event)
                .setMessage(redactMessage(event.getMessage()))
                .setThrown(sanitize(event.getThrown(), identitySet(), 0))
                .build();
        // Deliberately no setThrownProxy: it is a no-op stub in log4j-core 2.25 and the
        // copy-constructor does not carry the source event's proxy, so getThrownProxy() is
        // rebuilt lazily from the sanitised throwable above.
    }

    /**
     * Keeps the message's format string and scrubs its arguments.
     *
     * <p><b>Presence of parameters is the test, not the {@link Message} implementation type.</b>
     * Under {@code log4j2.enableThreadlocals} (true by default, and true in the Connect image) the
     * SLF4J bridge does not produce {@link ParameterizedMessage} at all - the event arrives as a
     * {@code MutableLogEvent}, which implements {@code Message} itself and is its own message. An
     * {@code instanceof ParameterizedMessage} check is therefore false for every real event,
     * which silently turns this whole method into "redact everything". Verified by probing a live
     * {@code RewritePolicy} inside the cp-kafka-connect image.
     *
     * <p>{@code getFormat()} is only a <em>pattern</em> when parameters exist. With none it is the
     * fully rendered text - for a concatenated call site such as {@code log.error("Failed: " +
     * record)} it returns {@code "Failed: key=jo.tan@example.com"}, PII included. So an empty
     * parameter array means redact wholesale; it is the signal that no structure is left to
     * exploit.
     *
     * <p>Residual risk, stated plainly: a call site that both concatenates and uses placeholders
     * ({@code log.warn("Failed: " + record + ", retries={}", n)}) yields a format string
     * containing data with a non-empty parameter array, and the format string would be kept. A
     * runtime String cannot be distinguished from a compile-time literal, so this is not
     * detectable here. No Connect framework logger does it; a third-party plugin could.
     */
    private Message redactMessage(final Message message) {
        if (message == null) {
            return new SimpleMessage(REDACTED);
        }
        // Read parameters before format: for a recycled reusable message both are only valid for
        // the duration of this call, and values are copied into a fresh array immediately below.
        final Object[] params = message.getParameters();
        if (params == null || params.length == 0) {
            return new SimpleMessage(REDACTED);
        }
        final String format = message.getFormat();
        if (format == null) {
            return new SimpleMessage(REDACTED);
        }
        // A format string is only a *pattern* if it actually has placeholders. Zero placeholders
        // with a non-empty parameter array is the concatenation-plus-throwable shape -
        // log.error("Failed: " + record, e) - where getFormat() is fully rendered text and the
        // lone parameter is the throwable. Keeping the format string there would leak it.
        final int placeholders = ParameterizedMessage.countArgumentPlaceholders(format);
        if (placeholders == 0) {
            return new SimpleMessage(REDACTED);
        }
        // Pass no more arguments than there are placeholders. SLF4J appends the throwable as a
        // trailing argument, so params is routinely one longer than the pattern expects; handing
        // the surplus to ParameterizedMessage makes log4j emit a StatusLogger warning per event.
        // The throwable is sanitised separately via setThrown.
        final int used = Math.min(placeholders, params.length);
        final Object[] scrubbed = new Object[used];
        for (int i = 0; i < used; i++) {
            scrubbed[i] = scrubParam(params[i]);
        }
        return new ParameterizedMessage(format, scrubbed);
    }

    /**
     * Allowlist. Numbers and booleans are the counts, offsets, partitions and retry budgets that
     * make a Connect log readable; everything else - String, SinkRecord, Struct, byte[] - is
     * assumed to carry data.
     *
     * <p>Residual risk, stated plainly: a numeric value that is itself sensitive (a salary, an
     * employee id) would survive if a call site passed it as a log argument. No Connect framework
     * logger does this, but a third-party plugin could. Redact all parameters if that is not an
     * acceptable residual.
     */
    private Object scrubParam(final Object param) {
        if (param == null) {
            return null;
        }
        if (param instanceof Number || param instanceof Boolean) {
            return param;
        }
        return REDACTED;
    }

    /**
     * Rebuilds the cause chain preserving class names and stack frames, dropping every message.
     *
     * <p>Does not branch on exception type - every throwable is handled identically, which is why
     * an unfamiliar exception from any connector cannot produce an unredacted message. Stack
     * frames are safe to keep in full: a {@code StackTraceElement} is declaring class, method,
     * file and line, all read from the class file's constant pool, so it cannot carry runtime
     * data.
     *
     * <p>Guarded by identity and depth: a self-referencing cause chain is legal Java and would
     * otherwise hang the logging thread.
     *
     * <p>Does not reflectively reconstruct the original exception type, which is what the
     * Confluent redactor does. That runs arbitrary driver constructors inside the logging path,
     * and many exception classes have no {@code (String)} constructor. Carrying the class name as
     * text is strictly safer and renders just as usefully.
     */
    private Throwable sanitize(final Throwable t, final Set<Throwable> seen, final int depth) {
        if (t == null || depth > MAX_CAUSE_DEPTH || !seen.add(t)) {
            return null;
        }
        // The cause must be passed to the constructor, never via initCause(): Throwable's 4-arg
        // constructor *sets* the cause field even when given null, after which initCause() throws
        // IllegalStateException("Can't overwrite cause").
        // Each accessor is guarded independently. getCause(), getMessage() and getStackTrace()
        // are all overridable, so a badly-behaved exception class from any plugin can throw from
        // any of them. Without per-field guards one such accessor would fail the whole event and
        // cost the entire class chain; with them we lose only the field that misbehaved.
        // getClass() and getSuppressed() are final in Throwable and cannot misbehave.
        Throwable cause = null;
        try {
            cause = sanitize(t.getCause(), seen, depth + 1);
        } catch (final Throwable hostileGetCause) {
            cause = null;
        }

        final Throwable safe = new RedactedThrowable(t.getClass().getName() + ": " + REDACTED, cause);

        if (keepStackFrames) {
            try {
                safe.setStackTrace(t.getStackTrace());
            } catch (final Throwable hostileGetStackTrace) {
                // Leave the frames RedactedThrowable was constructed with.
            }
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
        // Loud for the first occurrence then on powers of two, so a persistent fault stays visible
        // without flooding. Reported via StatusLogger, not a logger - going back through an
        // appender would recurse into this policy.
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
        return "PiiRedactionPolicy{redactAtOrAbove="
                + redactAtOrAbove
                + ", aggressiveAtOrAbove="
                + aggressiveAtOrAbove
                + ", aggressiveLoggers="
                + aggressiveLoggers
                + '}';
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
