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
 * Project-scoped rule. Fires on a kafka-clients consumer .properties
 * file that does NOT set {@code enable.auto.commit}. The kafka-clients
 * default is {@code true}, which schedules offset commits on a 5-second
 * timer REGARDLESS of processing success — at-most-once semantics
 * with silent data-loss on crash/rebalance. Right for fire-and-forget
 * (dashboards, metrics); wrong for at-least-once (analytics, ETL,
 * event-driven). WARNING severity because the default is workload-
 * dependent.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer}
 * OR {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * and Streams configs.
 */
public final class ConsumerPropertiesEnableAutoCommitAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";

    private final Severity severity;

    public ConsumerPropertiesEnableAutoCommitAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_ENABLE_AUTO_COMMIT_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String shapeKey = consumerShapeKey(p);
            if (shapeKey == null) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(ENABLE_AUTO_COMMIT))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_ENABLE_AUTO_COMMIT_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + ENABLE_AUTO_COMMIT, 0,
                    "kafka-clients consumer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + ENABLE_AUTO_COMMIT
                            + "`. kafka-clients defaults `" + ENABLE_AUTO_COMMIT
                            + "` to `true`, which commits offsets on a 5-second "
                            + "timer REGARDLESS of whether processing finished — "
                            + "at-most-once semantics with SILENT data-loss when "
                            + "the application crashes or a rebalance happens "
                            + "mid-batch. The default is the WRONG choice for the "
                            + "majority of consumer workloads (analytics, ETL, "
                            + "event-driven, anything requiring at-least-once "
                            + "delivery). The default is right ONLY for genuinely "
                            + "fire-and-forget consumers (live dashboards, health "
                            + "probes, metrics) where missed records are "
                            + "immaterial. Fix: set `" + ENABLE_AUTO_COMMIT
                            + "=false` for at-least-once (and call `commitSync()`/"
                            + "`commitAsync()` after processing), OR set it to "
                            + "`true` EXPLICITLY to document that auto-commit was "
                            + "deliberately chosen."));
        }
        return out;
    }

    private static String consumerShapeKey(Properties p) {
        if (isNonEmpty(p.getProperty(KEY_DESERIALIZER))) return KEY_DESERIALIZER;
        if (isNonEmpty(p.getProperty(VALUE_DESERIALIZER))) return VALUE_DESERIALIZER;
        return null;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
