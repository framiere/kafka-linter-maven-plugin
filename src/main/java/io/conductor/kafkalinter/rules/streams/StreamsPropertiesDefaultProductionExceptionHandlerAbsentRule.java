package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires on a Kafka Streams .properties file that
 * does NOT set {@code default.production.exception.handler}. The
 * Streams default is {@code DefaultProductionExceptionHandler} which
 * returns FAIL on every exception — one oversized output record
 * (RecordTooLargeException) kills the StreamThread and halts the
 * topology. INFO severity because fail-fast is defensible for some
 * workloads, but the unset case is overwhelmingly an accident.
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesDefaultProductionExceptionHandlerAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PROD_EXCEPTION_HANDLER = "default.production.exception.handler";

    private final Severity severity;

    public StreamsPropertiesDefaultProductionExceptionHandlerAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_DEFAULT_PRODUCTION_EXCEPTION_HANDLER_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(PROD_EXCEPTION_HANDLER))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_DEFAULT_PRODUCTION_EXCEPTION_HANDLER_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + PROD_EXCEPTION_HANDLER, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + PROD_EXCEPTION_HANDLER + "`. Streams defaults "
                            + "this to `DefaultProductionExceptionHandler` "
                            + "which returns `FAIL` for EVERY producer "
                            + "exception — `RecordTooLargeException`, "
                            + "`MessageSizeTooLargeException`, "
                            + "`UnknownTopicOrPartitionException`, "
                            + "`AuthorizationException`, exhausted-retry "
                            + "`TimeoutException`. The FAIL response "
                            + "propagates through `RecordCollectorImpl` as a "
                            + "`StreamsException`, killing the StreamThread; "
                            + "the topology stops processing the affected "
                            + "partition until a manual restart — which "
                            + "immediately re-fails on the same record. "
                            + "Common bug shape: a `KStream.join(KTable, ...)` "
                            + "output value (sum of left + right sizes) "
                            + "eventually exceeds the broker's "
                            + "`message.max.bytes=1048576` after the KTable "
                            + "accumulates more attributes; topology crashes "
                            + "with `RecordTooLargeException`. Fix: set "
                            + "`" + PROD_EXCEPTION_HANDLER + "=org.apache."
                            + "kafka.streams.errors.LogAndContinueProduction"
                            + "ExceptionHandler` (Kafka 3.9+ / KIP-1033 — "
                            + "skip + log + alert via `dropped-records` "
                            + "metric), or a custom DLQ handler. For fail-"
                            + "fast workloads (financial settlement, "
                            + "regulatory reporting), set "
                            + "`DefaultProductionExceptionHandler` "
                            + "EXPLICITLY to document the deliberate choice."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
