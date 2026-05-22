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
 * file that does NOT set {@code acks}. The kafka-clients default is
 * version-dependent ({@code 1} on 2.x, {@code all} on 3.x — KIP-679), so
 * an absent {@code acks} line silently inherits one of two different
 * durability contracts based on the kafka-clients JAR on the classpath.
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key (no framework prefix).
 * Excludes Spring Boot, Quarkus, Connect, and Streams configs — those
 * have their own dedicated scoped rules.
 */
public final class ProducerPropertiesAcksAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String ACKS = "acks";

    private final Severity severity;

    public ProducerPropertiesAcksAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_ACKS_ABSENT;
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
            if (isNonEmpty(p.getProperty(ACKS))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_ACKS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + ACKS, 0,
                    "kafka-clients producer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + ACKS + "`. The "
                            + "kafka-clients ProducerConfig default for `" + ACKS
                            + "` is VERSION-DEPENDENT: `1` on 2.x and earlier (leader-only "
                            + "durability — record lost on leader failover before "
                            + "replication completes), `all` on 3.x and later (per "
                            + "KIP-679 — waits for `min.insync.replicas` in-sync "
                            + "replicas). The same .properties file shipped against two "
                            + "different kafka-clients JAR versions gets two completely "
                            + "different durability contracts, silently. Fix: set `"
                            + ACKS + "=all` (recommended for production durability), or "
                            + "`" + ACKS + "=1` / `" + ACKS + "=0` with an explicit "
                            + "comment documenting the trade-off (low-latency / "
                            + "fire-and-forget telemetry). The choice MUST be in the "
                            + "file, not inherited from the kafka-clients JAR default."));
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
