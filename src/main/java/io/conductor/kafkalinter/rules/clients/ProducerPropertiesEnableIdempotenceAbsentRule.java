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
 * file that does NOT set {@code enable.idempotence}. The kafka-clients
 * default is version-dependent ({@code false} on 2.x, {@code true} on 3.x —
 * KIP-679), and KIP-679 also introduces silent-disable rules where an
 * effective-true idempotence is downgraded to false when other keys
 * conflict — but ONLY when the operator did NOT set the key explicitly.
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key (no framework prefix).
 * Excludes Connect connector configs (which deliberately default
 * idempotence false for broker-version breadth per KAFKA-13759) and
 * Streams configs.
 */
public final class ProducerPropertiesEnableIdempotenceAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String ENABLE_IDEMPOTENCE = "enable.idempotence";

    private final Severity severity;

    public ProducerPropertiesEnableIdempotenceAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_ENABLE_IDEMPOTENCE_ABSENT;
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
            if (isNonEmpty(p.getProperty(ENABLE_IDEMPOTENCE))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_ENABLE_IDEMPOTENCE_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + ENABLE_IDEMPOTENCE, 0,
                    "kafka-clients producer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + ENABLE_IDEMPOTENCE
                            + "`. The kafka-clients ProducerConfig default is "
                            + "VERSION-DEPENDENT: `false` on 2.x (retries can produce "
                            + "duplicates AND, with `max.in.flight > 1`, reorder writes "
                            + "per partition), `true` on 3.x+ (KIP-679 — dedupe + "
                            + "ordering guaranteed). The same .properties shipped against "
                            + "two different kafka-clients JAR versions has two different "
                            + "retry-semantics contracts, silently. Worse: even on 3.x, "
                            + "KIP-679's silent-disable matrix downgrades the effective "
                            + "value to false if `acks=0`/`1`, `max.in.flight > 5`, or "
                            + "`retries=0` is set — and the downgrade is a WARN log line "
                            + "nobody reads. Setting `" + ENABLE_IDEMPOTENCE + "=true` "
                            + "EXPLICITLY upgrades that silent WARN into a ConfigException "
                            + "at startup (loud, fail-fast). Fix: set `"
                            + ENABLE_IDEMPOTENCE + "=true` (recommended for every "
                            + "non-trivial producer), or `" + ENABLE_IDEMPOTENCE
                            + "=false` with an explicit comment documenting why "
                            + "at-least-once is acceptable for this stream."));
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
