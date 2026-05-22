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
import java.util.Set;

/**
 * Project-scoped rule. Fires when a MirrorMaker 2 connector config
 * ({@code connector.class} is one of the three MM2 connector classes) sets
 * any of the four MM2 replication-factor keys EXPLICITLY to an integer value
 * less than {@code 3}:
 * <ul>
 *   <li>{@code replication.factor} — replicated data topics on the target cluster</li>
 *   <li>{@code checkpoints.topic.replication.factor} — the MM2 checkpoints topic</li>
 *   <li>{@code heartbeats.topic.replication.factor} — the MM2 heartbeats topic</li>
 *   <li>{@code offset-syncs.topic.replication.factor} — the MM2 offset-syncs topic</li>
 * </ul>
 *
 * <p>Emits one violation per offending key per file.
 */
public final class Mm2InternalTopicReplicationFactorLowRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";

    private static final Set<String> MM2_CLASSES = Set.of(
            "org.apache.kafka.connect.mirror.MirrorSourceConnector",
            "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
            "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector"
    );

    private static final String REPLICATION_FACTOR = "replication.factor";
    private static final String CHECKPOINTS_RF = "checkpoints.topic.replication.factor";
    private static final String HEARTBEATS_RF = "heartbeats.topic.replication.factor";
    private static final String OFFSET_SYNCS_RF = "offset-syncs.topic.replication.factor";

    private static final int MIN_PRODUCTION_RF = 3;

    private final Severity severity;

    public Mm2InternalTopicReplicationFactorLowRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.MM2_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MM2_CLASSES.contains(connectorClass)) continue;

            String file = ctx.relativize(e.getKey());
            checkKey(out, p, file, connectorClass, REPLICATION_FACTOR,
                    "replicated DATA topics that MM2 auto-creates on the target "
                            + "cluster (one per replicated source topic, named "
                            + "<source-alias>.<topic>)");
            checkKey(out, p, file, connectorClass, CHECKPOINTS_RF,
                    "the MM2 checkpoints topic <source-alias>.checkpoints.internal "
                            + "— the failover-recovery substrate that stores translated "
                            + "consumer-group offsets; an unavailable checkpoints topic "
                            + "makes primary→DR cutover impossible until the topic comes "
                            + "back online");
            checkKey(out, p, file, connectorClass, HEARTBEATS_RF,
                    "the MM2 heartbeats topic — the observability substrate that "
                            + "downstream monitoring queries to confirm replication is "
                            + "alive; an unavailable heartbeats topic looks identical to "
                            + "an actual replication stoppage and can auto-page a false "
                            + "outage");
            checkKey(out, p, file, connectorClass, OFFSET_SYNCS_RF,
                    "the MM2 offset-syncs topic <source-alias>.offset-syncs.internal "
                            + "— the source→target offset-mapping substrate for the "
                            + "MirrorCheckpointConnector's offset-translation machinery; "
                            + "an unavailable offset-syncs topic starves the checkpoint "
                            + "connector and breaks failover");
        }
        return out;
    }

    private void checkKey(List<Violation> out, Properties p, String file,
                          String connectorClass, String key, String role) {
        String raw = trimOrNull(p.getProperty(key));
        if (raw == null) return;
        int value;
        try {
            value = Integer.parseInt(raw);
        } catch (NumberFormatException nfe) {
            return;
        }
        if (value >= MIN_PRODUCTION_RF) return;

        out.add(new Violation(
                RuleId.MM2_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW, severity,
                file, "key:" + key, 0,
                "MirrorMaker 2 connector (" + connectorClass + ") has " + key + "="
                        + value + " — this RF controls " + role + ". A value below 3 "
                        + "means a single broker outage on the target cluster takes the "
                        + "topic offline; for RF=1 specifically, ANY broker restart that "
                        + "holds the topic's partitions makes the topic unavailable for "
                        + "the duration of the restart. The dominant origin of low RF on "
                        + "MM2 internal topics is a dev-cluster default (single-broker "
                        + "dev cluster requires RF=1 to come up green) that ships to "
                        + "production via Helm-chart inheritance, GitOps repo promotion, "
                        + "or environment-overlay misconfiguration. The damage is "
                        + "asymmetric — costs nothing in steady state, exacts a steep "
                        + "price exactly when the operator needs the system to be "
                        + "durable (outages, drills, DR cutovers). Fix: set " + key
                        + "=3 (or to your target cluster's data-topic replication "
                        + "factor; 3 is the canonical answer for any production Kafka "
                        + "deployment). IMPORTANT: changing this config does NOT "
                        + "automatically alter the RF of topics that already exist — MM2 "
                        + "uses this value only for first-time topic creation. Existing "
                        + "topics retain their original RF; raising RF retroactively "
                        + "requires `kafka-reassign-partitions.sh` (a manual operation "
                        + "with shadowing during the reassignment window). Plan both the "
                        + "config fix AND the topic-reassignment together. If the "
                        + "deployment cluster genuinely has fewer than 3 brokers, MM2 "
                        + "should not be running on it — MM2's whole purpose is durable "
                        + "cross-cluster replication, which requires durable per-topic "
                        + "storage."));
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
