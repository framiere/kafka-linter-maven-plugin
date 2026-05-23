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
 * does NOT set {@code processing.exception.handler} (KIP-1033, Apache
 * Kafka 3.9, November 2024). The Streams default is
 * {@code LogAndFailProcessingExceptionHandler} which kills the
 * StreamThread on any {@code RuntimeException} thrown from user
 * lambda code (mapValues, transform, aggregate, custom Processor, etc.)
 * — and the same bad record re-throws on every pod restart, creating
 * an infinite restart loop. INFO severity because fail-fast is
 * defensible for some workloads (financial settlement, regulatory
 * reporting), but the unset case is overwhelmingly an accident.
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesProcessingExceptionHandlerAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PROCESSING_EXCEPTION_HANDLER = "processing.exception.handler";

    private final Severity severity;

    public StreamsPropertiesProcessingExceptionHandlerAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_PROCESSING_EXCEPTION_HANDLER_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(PROCESSING_EXCEPTION_HANDLER))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_PROCESSING_EXCEPTION_HANDLER_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + PROCESSING_EXCEPTION_HANDLER, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + PROCESSING_EXCEPTION_HANDLER + "`. KIP-1033 "
                            + "(Apache Kafka 3.9, November 2024) introduced "
                            + "this knob as the THIRD member of the Streams "
                            + "exception-handler family — alongside `default."
                            + "deserialization.exception.handler` (input-side, "
                            + "poison records on poll) and `default."
                            + "production.exception.handler` (output-side, "
                            + "broker rejection on produce). Where those two "
                            + "cover I/O boundaries, `processing.exception."
                            + "handler` covers the MIDDLE of the topology — "
                            + "exceptions thrown by USER PROCESSOR CODE in "
                            + "the lambdas of `mapValues`, `map`, `transform`, "
                            + "`transformValues`, `flatMap`, `filter`, "
                            + "`aggregate`, `reduce`, `join`'s ValueJoiner, "
                            + "and any custom `Processor` / `Transformer` / "
                            + "`ValueTransformer` implementation. The Streams "
                            + "default is `org.apache.kafka.streams.errors."
                            + "LogAndFailProcessingExceptionHandler` which "
                            + "returns `FAIL` for EVERY exception — equivalent "
                            + "to pre-KIP-1033 behavior: any "
                            + "`RuntimeException` from user code propagates "
                            + "up to the StreamThread, the configured "
                            + "uncaught-exception handler kills the thread, "
                            + "and (with default `SHUTDOWN_CLIENT` response) "
                            + "the entire KafkaStreams instance dies. The "
                            + "topology stops processing the affected "
                            + "partition; the pod restarts; the SAME bad "
                            + "record is re-polled (no offset commit "
                            + "happened); the SAME exception re-fires — "
                            + "infinite restart loop until someone manually "
                            + "advances the consumer offset past the bad "
                            + "record. Common bug shapes: (1) "
                            + "`mapValues(v -> objectMapper.readTree(v).get("
                            + "\"id\").asText())` — non-JSON record throws "
                            + "`JsonProcessingException`. (2) "
                            + "`transformValues((store, k, v) -> "
                            + "store.get(v.getKey()).getName())` — missing "
                            + "enrichment record throws NPE. (3) Custom "
                            + "`Processor` with `ClassCastException` on "
                            + "schema drift. (4) `aggregate((sum + v) / "
                            + "count)` — `ArithmeticException` on transient "
                            + "zero count. (5) `flatMap` iterating a list "
                            + "with `IndexOutOfBoundsException` on empty "
                            + "input. (6) Schema-evolution drift accessing "
                            + "a removed field — NPE on null. Fix: set "
                            + "`" + PROCESSING_EXCEPTION_HANDLER + "=org."
                            + "apache.kafka.streams.errors.LogAndContinue"
                            + "ProcessingExceptionHandler` (Kafka 3.9+ — "
                            + "skip + log + alert via `dropped-records` "
                            + "metric) for resilient workloads. For fail-"
                            + "fast workloads (financial settlement, "
                            + "regulatory reporting), set "
                            + "`LogAndFailProcessingExceptionHandler` "
                            + "EXPLICITLY to document the deliberate "
                            + "choice. For DLQ routing, implement a "
                            + "custom `ProcessingExceptionHandler` that "
                            + "returns `CONTINUE` after sending the "
                            + "record to a side-channel producer."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
