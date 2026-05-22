package io.conductor.kafkalinter.rules.connect;

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
 * Project-scoped rule. Fires when a Debezium connector (any class under
 * {@code io.debezium.connector.}) configures {@code max.batch.size} and
 * {@code max.queue.size} such that {@code max.batch.size >= max.queue.size}.
 *
 * <p>Debezium's documentation explicitly requires
 * {@code max.queue.size > max.batch.size}: the bounded queue between the
 * database-reader thread and the Connect-framework consumer thread must hold
 * at least one full batch. Violating the invariant collapses throughput
 * (poll cycles drain at most {@code max.queue.size} records, not
 * {@code max.batch.size}) and can produce queue deadlock when out-of-band
 * heartbeat/metadata events compete for queue space. Defaults are
 * {@code max.queue.size=8192} and {@code max.batch.size=2048}.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumMaxQueueSizeLessThanMaxBatchSizeRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";
    private static final String MAX_QUEUE_SIZE = "max.queue.size";
    private static final String MAX_BATCH_SIZE = "max.batch.size";
    private static final int DEFAULT_MAX_QUEUE_SIZE = 8192;
    private static final int DEFAULT_MAX_BATCH_SIZE = 2048;

    private final Severity severity;

    public ConnectDebeziumMaxQueueSizeLessThanMaxBatchSizeRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_MAX_QUEUE_SIZE_LESS_THAN_MAX_BATCH_SIZE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;

            Integer queueSize = parsePositiveInt(p.getProperty(MAX_QUEUE_SIZE));
            Integer batchSize = parsePositiveInt(p.getProperty(MAX_BATCH_SIZE));

            // Only fire when at least one of the two keys is explicitly set —
            // otherwise both defaults (8192, 2048) satisfy the invariant.
            if (queueSize == null && batchSize == null) continue;

            int effectiveQueue = queueSize != null ? queueSize : DEFAULT_MAX_QUEUE_SIZE;
            int effectiveBatch = batchSize != null ? batchSize : DEFAULT_MAX_BATCH_SIZE;

            if (effectiveBatch < effectiveQueue) continue;

            String anchorKey = batchSize != null ? MAX_BATCH_SIZE : MAX_QUEUE_SIZE;
            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_MAX_QUEUE_SIZE_LESS_THAN_MAX_BATCH_SIZE, severity,
                    ctx.relativize(e.getKey()), "key:" + anchorKey, 0,
                    "Debezium connector " + connectorClass
                            + " has max.batch.size=" + effectiveBatch
                            + " and max.queue.size=" + effectiveQueue
                            + (queueSize == null ? " (default)" : "")
                            + (batchSize == null ? " (max.batch.size default)" : "")
                            + " — the invariant `max.queue.size > max.batch.size` is VIOLATED. "
                            + "Debezium's record-flow pipeline runs the database reader and the "
                            + "Connect-framework consumer on different threads, connected by a "
                            + "bounded queue of capacity max.queue.size; the consumer polls up to "
                            + "max.batch.size records per cycle. The queue must be STRICTLY "
                            + "larger than the batch — otherwise the consumer can never drain a "
                            + "full batch in one poll, throughput collapses to ~max.queue.size "
                            + "records per cycle, and out-of-band heartbeat/metadata events "
                            + "compete for queue space producing sporadic reader-thread blocking. "
                            + "Debezium docs explicitly state max.queue.size 'should always be "
                            + "larger than the maximum batch size.' Fix by either reducing "
                            + "max.batch.size below max.queue.size, or increasing max.queue.size "
                            + "above max.batch.size; Debezium's tuning guide suggests "
                            + "max.queue.size >= 2 * max.batch.size for robustness."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static Integer parsePositiveInt(String v) {
        String t = trimOrNull(v);
        if (t == null) return null;
        try {
            int n = Integer.parseInt(t);
            return n > 0 ? n : null;
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
