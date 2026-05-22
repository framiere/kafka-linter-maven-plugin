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
 * file (identified by top-level {@code key.deserializer} or
 * {@code value.deserializer}) that does NOT set {@code heartbeat.interval.ms}.
 *
 * <p>kafka-clients' {@code ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG}
 * defaults to {@code 3000} (3 seconds). Together with
 * {@code session.timeout.ms} (broker-side liveness window), the heartbeat
 * forms a single COUPLED operational knob: heartbeats must arrive faster
 * than the session timeout expires. The well-known guideline is
 * {@code heartbeat.interval.ms <= session.timeout.ms / 3}.
 *
 * <p>When the operator tunes {@code session.timeout.ms} without also tuning
 * {@code heartbeat.interval.ms}, the coupling breaks: too-aggressive
 * heartbeats waste broker capacity (e.g., 20 heartbeats per session window
 * with default 3s heartbeat + 60s session timeout); too-narrow ratios cause
 * false-positive rebalances under GC pauses.
 *
 * <p>INFO severity because the defaults work for default settings; the rule
 * is a documentation nudge prompting the operator to set heartbeat AND
 * session.timeout TOGETHER, documenting the coupling explicitly.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer} OR
 * {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * ({@code connector.class}) and Streams ({@code application.id}).
 */
public final class ConsumerPropertiesHeartbeatIntervalMsAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String HEARTBEAT_INTERVAL_MS = "heartbeat.interval.ms";

    private final Severity severity;

    public ConsumerPropertiesHeartbeatIntervalMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_HEARTBEAT_INTERVAL_MS_ABSENT;
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
            if (isNonEmpty(p.getProperty(HEARTBEAT_INTERVAL_MS))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_HEARTBEAT_INTERVAL_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + HEARTBEAT_INTERVAL_MS, 0,
                    "kafka-clients consumer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + HEARTBEAT_INTERVAL_MS + "`. kafka-clients "
                            + "defaults `" + HEARTBEAT_INTERVAL_MS
                            + "` to `3000` (3 seconds). Together with "
                            + "`session.timeout.ms` (broker-side liveness "
                            + "window, default 45s on 3.x via KIP-735, 10s "
                            + "on 2.x), the heartbeat is one half of a "
                            + "single COUPLED liveness knob: heartbeats "
                            + "must arrive faster than the session timeout, "
                            + "otherwise the coordinator declares the "
                            + "consumer dead and triggers a rebalance. The "
                            + "well-known guideline is `heartbeat.interval."
                            + "ms <= session.timeout.ms / 3` so that 2-3 "
                            + "heartbeats can be missed before the "
                            + "coordinator gives up — this absorbs GC "
                            + "pauses, transient broker-side coordinator "
                            + "slowness, and short network glitches without "
                            + "false-positive rebalances. Specific impacts "
                            + "of the absent key: (a) **operator tuned "
                            + "session.timeout.ms but not heartbeat** — "
                            + "default 3s heartbeat with tuned-up session "
                            + "timeout (e.g., 60s) wastes broker capacity "
                            + "(20 heartbeats per session window vs the "
                            + "recommended 2-3); default 3s heartbeat with "
                            + "tuned-down session timeout (e.g., 6s) gives "
                            + "a fragile 1:2 ratio that triggers false "
                            + "rebalances under GC pauses. (b) **silent "
                            + "version-coupled behavior** — the same "
                            + ".properties on kafka-clients 2.x runs 1:3 "
                            + "ratio (3s heartbeat / 10s default session "
                            + "timeout), on 3.x runs 1:15 ratio (3s / 45s "
                            + "default) — same code, different broker load "
                            + "shape across client versions. Common bug "
                            + "shapes: (1) team sets `session.timeout.ms="
                            + "120000` for a slow consumer, leaves "
                            + "heartbeat default; 40 heartbeats per "
                            + "session-window per consumer instance; "
                            + "multi-tenant coordinator sees 1000s of "
                            + "wasted heartbeats; (2) SRE-mandated fast "
                            + "failover sets `session.timeout.ms=6000`; "
                            + "default 3s heartbeat → 1:2 ratio → "
                            + "frequent false-positive rebalances under "
                            + "G1GC mixed-gen stops; (3) 2.x → 3.x upgrade "
                            + "silently flips the ratio from 1:3 to 1:15 "
                            + "with no .properties change; (4) Spring Boot "
                            + "runbook only mentions session.timeout, "
                            + "team forgets heartbeat; documentation gap "
                            + "leads to coupling break. Fix: ONE LINE. Set "
                            + "`" + HEARTBEAT_INTERVAL_MS
                            + "` to ~1/3 of `session.timeout.ms`. "
                            + "Examples: `" + HEARTBEAT_INTERVAL_MS
                            + "=15000` with `session.timeout.ms=45000` "
                            + "(default 3.x ratio, conservative); `"
                            + HEARTBEAT_INTERVAL_MS + "=3000` with "
                            + "`session.timeout.ms=10000` (tight, fast "
                            + "failover); `" + HEARTBEAT_INTERVAL_MS
                            + "=30000` with `session.timeout.ms=90000` "
                            + "(slow batch consumer). Always set BOTH "
                            + "together so future readers see the "
                            + "coupling explicitly. Sibling rule [[consumer-"
                            + "properties-session-timeout-ms-absent]] "
                            + "catches the coupled-partner absence; "
                            + "[[consumer-heartbeat-session-ratio]] catches "
                            + "the explicit-bad-ratio case."));
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
