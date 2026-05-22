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
 * Project-scoped rule. Fires when a MirrorMaker 2 source-connector config
 * ({@code connector.class=org.apache.kafka.connect.mirror.MirrorSourceConnector})
 * EXPLICITLY sets {@code tasks.max=1}. Single-task replication is the
 * throughput-bottleneck anti-pattern for MM2 — one consumer/producer pair
 * cannot keep up with multi-partition, multi-topic replication at scale.
 *
 * <p>Does NOT fire on {@code MirrorCheckpointConnector} or
 * {@code MirrorHeartbeatConnector} — both have minimal per-partition work
 * and {@code tasks.max=1} is correct for them.
 */
public final class Mm2SourceConnectorTasksMaxOneRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MIRROR_SOURCE_CONNECTOR =
            "org.apache.kafka.connect.mirror.MirrorSourceConnector";
    private static final String TASKS_MAX = "tasks.max";

    private final Severity severity;

    public Mm2SourceConnectorTasksMaxOneRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.MM2_SOURCE_CONNECTOR_TASKS_MAX_ONE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MIRROR_SOURCE_CONNECTOR.equals(connectorClass)) continue;

            String raw = trimOrNull(p.getProperty(TASKS_MAX));
            if (raw == null) continue;

            int value;
            try {
                value = Integer.parseInt(raw);
            } catch (NumberFormatException nfe) {
                continue;
            }
            if (value != 1) continue;

            String file = ctx.relativize(e.getKey());
            out.add(new Violation(
                    RuleId.MM2_SOURCE_CONNECTOR_TASKS_MAX_ONE, severity,
                    file, "key:" + TASKS_MAX, 0,
                    "MirrorMaker 2 source connector (" + connectorClass + ") has "
                            + TASKS_MAX + "=1 — this SERIALIZES all record "
                            + "replication through a SINGLE Connect task. "
                            + "`MirrorSourceConnector#taskConfigs(int maxTasks)` "
                            + "splits the replication of N source topic-partitions "
                            + "into M tasks; with `tasks.max=1`, M=1 and ALL "
                            + "partitions land on task 0. Task 0 runs a single "
                            + "Kafka consumer (against the source cluster) and a "
                            + "single Kafka producer (against the target cluster) "
                            + "in one thread; its throughput ceiling is one "
                            + "consumer/producer pair (typically 50–200 MB/s on "
                            + "typical brokers with TLS, replication.factor=3). On "
                            + "production MM2 deployments — which by definition "
                            + "replicate many partitions across many topics — this "
                            + "is the throughput bottleneck of the entire pipeline. "
                            + "Damage is silent in steady-state (replication "
                            + "appears to work) and emerges under load: traffic "
                            + "spikes, post-restart catch-up, multi-source "
                            + "amplification. The typical wrong value (the literal "
                            + "`1`) is almost always: (a) tutorial copy-paste — "
                            + "most MM2 tutorials use `tasks.max=1` 'to keep the "
                            + "example simple'; (b) Helm-chart inheritance from a "
                            + "generic Kafka-Connect template that defaults to 1 "
                            + "to match the Connect ConfigDef default; (c) "
                            + "confusion with MirrorHeartbeatConnector or "
                            + "MirrorCheckpointConnector (where `tasks.max=1` IS "
                            + "correct because both have minimal per-partition "
                            + "workload — one record per group or per heartbeat-"
                            + "interval); (d) a 'tune later' deferral that was "
                            + "never prioritized. Fix: set `tasks.max` to a value "
                            + "matching the desired parallelism — typical "
                            + "production starting point is 4–8 tasks per source "
                            + "cluster (e.g., `tasks.max=4`), scaled up based on "
                            + "observed replication lag and per-task throughput. "
                            + "Detection: trim, exact integer parse of `tasks.max`; "
                            + "fires only when the parsed value is exactly 1; "
                            + "exact class match against MirrorSourceConnector. "
                            + "The rule does NOT fire when `tasks.max` is absent "
                            + "(the absent case is ambiguous — could be Helm "
                            + "templating or env-var substitution intent). The "
                            + "rule does NOT fire on MirrorCheckpointConnector or "
                            + "MirrorHeartbeatConnector. If the operator "
                            + "deliberately wants single-task replication on a "
                            + "tiny dev cluster, suppress with "
                            + "`<MM2_SOURCE_CONNECTOR_TASKS_MAX_ONE>OFF</MM2_SOURCE_CONNECTOR_TASKS_MAX_ONE>` "
                            + "in the plugin config with documented rationale."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
