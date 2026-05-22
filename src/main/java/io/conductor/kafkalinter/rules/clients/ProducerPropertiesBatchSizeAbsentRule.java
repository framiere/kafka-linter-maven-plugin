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
 * file that does NOT set {@code batch.size}. The kafka-clients default is
 * {@code 16384} (16 KiB) — defensible but conservative; INFO severity
 * because the default is workable, not broken.
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key. Excludes Connect and
 * Streams configs.
 */
public final class ProducerPropertiesBatchSizeAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String BATCH_SIZE = "batch.size";

    private final Severity severity;

    public ProducerPropertiesBatchSizeAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_BATCH_SIZE_ABSENT;
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
            if (isNonEmpty(p.getProperty(BATCH_SIZE))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_BATCH_SIZE_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + BATCH_SIZE, 0,
                    "kafka-clients producer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + BATCH_SIZE
                            + "`. kafka-clients defaults `" + BATCH_SIZE
                            + "` to `16384` (16 KiB) — defensible but conservative. "
                            + "For high-throughput producers (>1k records/sec at "
                            + "non-trivial record sizes), raising to 65536-524288 "
                            + "amortizes per-batch protocol overhead across more "
                            + "records. Note the interaction with `linger.ms`: with "
                            + "`linger.ms=0`, raising batch.size has NO effect (every "
                            + "record ships immediately regardless of batch fullness); "
                            + "only with `linger.ms > 0` does a larger batch.size "
                            + "actually enable larger batches. Fix: set `" + BATCH_SIZE
                            + "=65536` (or a measured value matching the producer's "
                            + "throughput), or set it to `16384` EXPLICITLY to document "
                            + "that the default was deliberately chosen."));
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
