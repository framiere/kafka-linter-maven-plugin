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
 * EXPLICITLY sets {@code sync.topic.configs.enabled=false}. The default for this
 * key is {@code true}; opting out causes target-cluster replicated topics to
 * use target broker defaults and silently diverge from source topics on
 * cleanup.policy, retention.ms, compression.type, max.message.bytes, etc.
 *
 * <p>Only {@code MirrorSourceConnector} reads this key — the other two MM2
 * connectors ({@code MirrorCheckpointConnector}, {@code MirrorHeartbeatConnector})
 * do not own topic-config sync.
 */
public final class Mm2SyncTopicConfigsDisabledRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MIRROR_SOURCE_CONNECTOR =
            "org.apache.kafka.connect.mirror.MirrorSourceConnector";
    private static final String SYNC_TOPIC_CONFIGS_ENABLED = "sync.topic.configs.enabled";

    private final Severity severity;

    public Mm2SyncTopicConfigsDisabledRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.MM2_SYNC_TOPIC_CONFIGS_DISABLED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MIRROR_SOURCE_CONNECTOR.equals(connectorClass)) continue;

            String raw = trimOrNull(p.getProperty(SYNC_TOPIC_CONFIGS_ENABLED));
            if (raw == null) continue;
            if (!"false".equalsIgnoreCase(raw)) continue;

            String file = ctx.relativize(e.getKey());
            out.add(new Violation(
                    RuleId.MM2_SYNC_TOPIC_CONFIGS_DISABLED, severity,
                    file, "key:" + SYNC_TOPIC_CONFIGS_ENABLED, 0,
                    "MirrorMaker 2 source connector (" + connectorClass + ") has "
                            + SYNC_TOPIC_CONFIGS_ENABLED + "=" + raw + " — this OPTS OUT "
                            + "of topic-config synchronization between the source cluster and "
                            + "the target cluster. The default value is `true` because the "
                            + "dominant MM2 use case is DR replication, where target-cluster "
                            + "replicated topics MUST behave identically to source-cluster "
                            + "topics for consumers to fail over cleanly. Topic configs "
                            + "(cleanup.policy, retention.ms, compression.type, "
                            + "max.message.bytes, segment.ms, min.insync.replicas, etc.) are "
                            + "part of the topic's CONTRACT with its consumers; a config "
                            + "mismatch between source and target means consumers see "
                            + "different topic behavior depending on which cluster they're "
                            + "connected to. Two failure shapes: (1) SILENT DIVERGENCE AT "
                            + "CREATION — when MM2 auto-creates a replicated topic on the "
                            + "target, it uses target broker defaults rather than copying "
                            + "source-topic configs; a source topic with "
                            + "cleanup.policy=compact silently becomes cleanup.policy=delete "
                            + "on the target (or vice versa); compacted topics on the source "
                            + "lose their compaction semantics on the target, and consumers "
                            + "that depend on 'latest value per key' semantics break during "
                            + "failover. (2) SILENT ONGOING DIVERGENCE — when an operator "
                            + "runs `kafka-configs.sh --alter` on the source cluster (e.g., "
                            + "raising retention.ms from 7d to 30d for a compliance "
                            + "requirement), the change does NOT propagate to the target; the "
                            + "target retains the old retention; DR consumers reading historic "
                            + "data see records expire earlier than expected. Both failure "
                            + "shapes are invisible in steady-state monitoring (replication "
                            + "throughput dashboards show records flowing; nothing alerts) and "
                            + "emerge only during DR drills, compliance audits, or actual "
                            + "cluster failover. Fix: remove the explicit `=false` (the "
                            + "default `=true` is the production-recommended setting), OR if "
                            + "the operator deliberately wants per-cluster config divergence "
                            + "(e.g., target cluster has different storage tier and lower "
                            + "retention for cost reasons), document the intent and suppress "
                            + "this rule via per-rule severity override "
                            + "(`<MM2_SYNC_TOPIC_CONFIGS_DISABLED>OFF</MM2_SYNC_TOPIC_CONFIGS_DISABLED>` "
                            + "in the plugin config). Default severity is WARNING (not ERROR) "
                            + "because the legitimate per-cluster-config-divergence use case "
                            + "exists. NOTE: enabling `sync.topic.configs.enabled=true` does "
                            + "NOT retroactively fix already-divergent topics — MM2 syncs "
                            + "configs going forward, but existing target topics keep their "
                            + "current configs until manually aligned with "
                            + "`kafka-configs.sh --alter` on the target cluster."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
