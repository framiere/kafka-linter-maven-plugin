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
 * file that does NOT set {@code max.poll.interval.ms}. The kafka-
 * clients default is {@code 300000} (5 minutes) — generous for
 * typical workloads, tight for slow-processing consumers (LLM
 * inference, external HTTP, bulk DB writes). INFO severity because
 * the default is defensible for typical workloads.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer}
 * OR {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * and Streams configs.
 */
public final class ConsumerPropertiesMaxPollIntervalMsAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String MAX_POLL_INTERVAL_MS = "max.poll.interval.ms";

    private final Severity severity;

    public ConsumerPropertiesMaxPollIntervalMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_MAX_POLL_INTERVAL_MS_ABSENT;
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
            if (isNonEmpty(p.getProperty(MAX_POLL_INTERVAL_MS))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_MAX_POLL_INTERVAL_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + MAX_POLL_INTERVAL_MS, 0,
                    "kafka-clients consumer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + MAX_POLL_INTERVAL_MS
                            + "`. kafka-clients defaults `" + MAX_POLL_INTERVAL_MS
                            + "` to `300000` (5 minutes) — the broker's patience "
                            + "for a consumer to call `poll()` again before "
                            + "declaring it dead and rebalancing. The default is "
                            + "generous for typical workloads but tight for slow-"
                            + "processing consumers (LLM inference, external HTTP, "
                            + "bulk DB writes). Pairing constraint: "
                            + "`max.poll.records × p99-per-record-cost < "
                            + "max.poll.interval.ms × 0.5`. With defaults this "
                            + "gives ~300 ms per-record budget; for slower "
                            + "workloads the operator must either raise this "
                            + "interval or lower `max.poll.records`. Fix: set `"
                            + MAX_POLL_INTERVAL_MS + "` to a value matched to "
                            + "worst-case batch time + safety margin, OR set it "
                            + "to `300000` EXPLICITLY to document that the "
                            + "default was deliberately chosen."));
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
