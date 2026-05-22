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
 * file that does NOT set {@code session.timeout.ms}. The kafka-
 * clients default is CLIENT-VERSION-DEPENDENT: 10000 on 2.x, 45000
 * on 3.0+. A client-library upgrade silently changes rebalance
 * behavior. INFO severity because the default is defensible for
 * typical workloads on either version.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer}
 * OR {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * and Streams configs.
 */
public final class ConsumerPropertiesSessionTimeoutMsAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String SESSION_TIMEOUT_MS = "session.timeout.ms";

    private final Severity severity;

    public ConsumerPropertiesSessionTimeoutMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_SESSION_TIMEOUT_MS_ABSENT;
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
            if (isNonEmpty(p.getProperty(SESSION_TIMEOUT_MS))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_SESSION_TIMEOUT_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + SESSION_TIMEOUT_MS, 0,
                    "kafka-clients consumer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + SESSION_TIMEOUT_MS
                            + "`. kafka-clients defaults `" + SESSION_TIMEOUT_MS
                            + "` to a CLIENT-VERSION-DEPENDENT value: `10000` "
                            + "(10 s) on kafka-clients 2.x, `45000` (45 s) on "
                            + "kafka-clients 3.0+ (KIP-735). This is the broker's "
                            + "patience for the consumer's heartbeat thread "
                            + "before declaring it dead — different from "
                            + "`max.poll.interval.ms`, which measures the "
                            + "application thread's poll cadence. A client-"
                            + "library upgrade SILENTLY changes the eviction "
                            + "behavior: a 30-second GC pause that USED to "
                            + "evict (2.x default 10 s) now silently passes "
                            + "(3.x default 45 s); a `heartbeat.interval.ms=15000` "
                            + "guide-snippet that holds the `<= session.timeout.ms / 3` "
                            + "constraint on 3.x is REJECTED at join time on "
                            + "2.x. Fix: set `" + SESSION_TIMEOUT_MS + "` to a "
                            + "value matched to network-blip tolerance and "
                            + "paired against `heartbeat.interval.ms` (typical: "
                            + "`30000` with heartbeat `3000` = 10× headroom), "
                            + "OR set it to the version's default EXPLICITLY "
                            + "to document that the default was deliberately "
                            + "chosen."));
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
