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
 * Project-scoped rule. Fires on a plain kafka-clients consumer .properties
 * file that does NOT set {@code auto.offset.reset}. The kafka-clients
 * default is {@code latest} — silently skips data produced before the
 * consumer group's first join time. For analytics, replay, and event-
 * driven workloads, {@code earliest} is almost always the right answer;
 * the default {@code latest} is right only for monitoring/heartbeat/
 * probe workloads. WARNING severity because the default is workload-
 * dependent and the rule cannot determine intent from absence alone.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer}
 * OR {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * and Streams configs.
 */
public final class ConsumerPropertiesAutoOffsetResetAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String AUTO_OFFSET_RESET = "auto.offset.reset";

    private final Severity severity;

    public ConsumerPropertiesAutoOffsetResetAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_AUTO_OFFSET_RESET_ABSENT;
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
            if (isNonEmpty(p.getProperty(AUTO_OFFSET_RESET))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_AUTO_OFFSET_RESET_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + AUTO_OFFSET_RESET, 0,
                    "kafka-clients consumer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + AUTO_OFFSET_RESET
                            + "`. kafka-clients defaults `" + AUTO_OFFSET_RESET
                            + "` to `latest` — when this consumer group joins a "
                            + "partition for the FIRST time (no committed offset "
                            + "exists), it JUMPS to the end of the log, SILENTLY "
                            + "SKIPPING all records produced before the join. The "
                            + "default is the WRONG choice for the majority of "
                            + "consumer workloads (analytics, replay, event-driven "
                            + "applications all want `earliest`); the default is "
                            + "right ONLY for monitoring/heartbeat/health-probe "
                            + "consumers. The classic 'why didn't we get message X' "
                            + "ticket is almost always this. Fix: set `"
                            + AUTO_OFFSET_RESET + "=earliest` for replay/analytics/"
                            + "event-driven workloads, `" + AUTO_OFFSET_RESET
                            + "=latest` for monitoring (explicitly, to document the "
                            + "choice), or `" + AUTO_OFFSET_RESET + "=none` for "
                            + "systems that MUST be explicitly bootstrapped."));
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
