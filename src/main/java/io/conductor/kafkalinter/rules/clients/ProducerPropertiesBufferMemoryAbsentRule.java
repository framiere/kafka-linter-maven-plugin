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
 * file that does NOT set {@code buffer.memory}. The kafka-clients default
 * is {@code 33554432} (32 MiB) — sensible for typical workloads, but
 * inadequate for high-partition-count / high-record-size / raised-
 * batch.size deployments, and excessive for memory-constrained containers.
 * INFO severity because the default is workable, not broken.
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key. Excludes Connect and
 * Streams configs.
 */
public final class ProducerPropertiesBufferMemoryAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String BUFFER_MEMORY = "buffer.memory";

    private final Severity severity;

    public ProducerPropertiesBufferMemoryAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_BUFFER_MEMORY_ABSENT;
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
            if (isNonEmpty(p.getProperty(BUFFER_MEMORY))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_BUFFER_MEMORY_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + BUFFER_MEMORY, 0,
                    "kafka-clients producer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + BUFFER_MEMORY
                            + "`. kafka-clients defaults `" + BUFFER_MEMORY
                            + "` to `33554432` (32 MiB) — the total accumulator memory "
                            + "the producer is allowed to use across all partitions. "
                            + "When the pool is exhausted, `send()` BLOCKS the caller "
                            + "for up to `max.block.ms` then throws TimeoutException. "
                            + "The default is sensible for typical workloads but: "
                            + "(a) inadequate for high-partition-count (>~500) "
                            + "producers, high-record-size workloads, or raised "
                            + "`batch.size`; (b) excessive for memory-constrained "
                            + "containers (k8s memory limits, embedded JVMs). The "
                            + "value should be ≥ `batch.size × num.partitions × "
                            + "in-flight-batches + headroom` AND ≤ container memory "
                            + "budget. Fix: set `" + BUFFER_MEMORY
                            + "` to a value matched to the workload, OR set it to "
                            + "`33554432` EXPLICITLY to document that the default was "
                            + "deliberately chosen."));
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
