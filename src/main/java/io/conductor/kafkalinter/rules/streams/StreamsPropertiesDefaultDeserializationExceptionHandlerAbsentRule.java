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
 * does NOT set {@code default.deserialization.exception.handler}. The
 * Streams default is {@code LogAndFailExceptionHandler} — one poison
 * record (corrupt bytes, schema drift) kills the entire topology.
 * INFO severity because some workloads genuinely need fail-fast,
 * but the unset case is overwhelmingly an accident.
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesDefaultDeserializationExceptionHandlerAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DESER_EXCEPTION_HANDLER = "default.deserialization.exception.handler";

    private final Severity severity;

    public StreamsPropertiesDefaultDeserializationExceptionHandlerAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(DESER_EXCEPTION_HANDLER))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + DESER_EXCEPTION_HANDLER, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + DESER_EXCEPTION_HANDLER + "`. Streams defaults "
                            + "this to `LogAndFailExceptionHandler` — when "
                            + "Streams cannot deserialize an input record "
                            + "(corrupt bytes, Avro schema drift, wrong "
                            + "Serde), the handler logs AND throws, which "
                            + "crashes the StreamThread. With default `num."
                            + "stream.threads=1`, the entire topology stops. "
                            + "Common bug shape: upstream producer ships a "
                            + "new optional Avro field (FORWARD-compatible "
                            + "in Schema Registry), but Streams' Serde "
                            + "rejects it; topology dies at 3 AM, pod auto-"
                            + "restarts, re-reads the same poison offset, "
                            + "crashes again → infinite restart loop until "
                            + "operator manually advances offset. Fix: set "
                            + "`" + DESER_EXCEPTION_HANDLER + "=org.apache."
                            + "kafka.streams.errors.LogAndContinueException"
                            + "Handler` (skip + log + alert via `skipped-"
                            + "records-total` metric), or a custom DLQ "
                            + "handler. For fail-fast workloads (financial "
                            + "settlement, regulatory reporting), set "
                            + "`LogAndFailExceptionHandler` EXPLICITLY to "
                            + "document the deliberate choice."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
