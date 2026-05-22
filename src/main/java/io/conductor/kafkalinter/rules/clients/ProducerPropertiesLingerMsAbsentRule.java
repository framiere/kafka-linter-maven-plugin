package io.conductor.kafkalinter.rules.clients;

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
 * Project-scoped rule. Fires on a plain kafka-clients producer .properties
 * file that does NOT set {@code linger.ms}. The kafka-clients default is
 * {@code 0} — no batching, one ProduceRequest per record on a steady
 * stream — and has been zero in every kafka-clients release.
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key (no framework prefix).
 * Excludes Connect and Streams configs.
 */
public final class ProducerPropertiesLingerMsAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String LINGER_MS = "linger.ms";

    private final Severity severity;

    public ProducerPropertiesLingerMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_LINGER_MS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String shapeKey = producerShapeKey(p);
            if (shapeKey == null) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(LINGER_MS))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_LINGER_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + LINGER_MS, 0,
                    "kafka-clients producer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + LINGER_MS
                            + "`. kafka-clients defaults `" + LINGER_MS + "` to `0` "
                            + "— the producer's accumulator sends partial batches "
                            + "AS SOON as the sender thread sees them, meaning every "
                            + "record becomes its own ProduceRequest on a steady "
                            + "stream of small records. Bandwidth efficiency drops "
                            + "10-100× compared to even a tiny `" + LINGER_MS
                            + "=5`; broker CPU is dominated by per-request handling "
                            + "overhead; cluster scaling is forced by producer-driven "
                            + "request rate, not record-byte throughput. Fix: set `"
                            + LINGER_MS + "=5` (the smallest non-zero value with "
                            + "measurable batching benefit), `" + LINGER_MS + "=20` "
                            + "(a generally safe default for high-fanout workloads), "
                            + "or `" + LINGER_MS + "=0` EXPLICITLY with a comment "
                            + "documenting why latency-criticality outweighs the "
                            + "throughput cost."));
        }
        return out;
    }

    private static String producerShapeKey(Properties p) {
        if (isNonEmpty(p.getProperty(KEY_SERIALIZER))) return KEY_SERIALIZER;
        if (isNonEmpty(p.getProperty(VALUE_SERIALIZER))) return VALUE_SERIALIZER;
        return null;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
