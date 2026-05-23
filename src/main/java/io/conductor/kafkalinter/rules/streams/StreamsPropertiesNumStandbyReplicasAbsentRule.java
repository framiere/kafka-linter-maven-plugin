package io.conductor.kafkalinter.rules.streams;

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
 * Project-scoped rule. Fires on a Kafka Streams .properties file
 * (identified by top-level {@code application.id}, excluding Connect
 * configs via {@code connector.class}) that does NOT set
 * {@code num.standby.replicas}.
 *
 * <p>Streams defaults {@code num.standby.replicas} to {@code 0}
 * (declared in {@code StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG},
 * {@code ConfigDef.Type.INT}). With {@code 0}, NO warm copies of any
 * state-store partition are maintained on peer instances — meaning ANY
 * instance crash forces the new task owner to replay the entire
 * changelog topic from earliest before resuming processing. For a
 * stateful Streams app with non-trivial state, restoration can take
 * 30 minutes to several hours.
 *
 * <p>WARNING severity because the failure mode is operational
 * degradation (multi-hour lag spikes on restarts), not data loss —
 * but it's a high-visibility ops incident that almost always traces
 * back to this absent key. The default {@code 0} is correct ONLY for
 * stateless apps.
 *
 * <p>Complementary to {@link io.conductor.kafkalinter.rules.config.ConfigKeyValueRule}-based
 * {@code STREAMS_NUM_STANDBY_REPLICAS_ZERO} which catches the
 * bytecode-side explicit {@code =0} value.
 */
public final class StreamsPropertiesNumStandbyReplicasAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String NUM_STANDBY_REPLICAS = "num.standby.replicas";

    private final Severity severity;

    public StreamsPropertiesNumStandbyReplicasAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_NUM_STANDBY_REPLICAS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(NUM_STANDBY_REPLICAS))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_NUM_STANDBY_REPLICAS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + NUM_STANDBY_REPLICAS, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + NUM_STANDBY_REPLICAS + "`. Streams defaults `"
                            + NUM_STANDBY_REPLICAS + "` to `0` — meaning NO "
                            + "warm copies of any state-store partition are "
                            + "maintained on peer instances. Any instance "
                            + "crash (JVM OOM, pod eviction, rolling deploy) "
                            + "forces the new task owner to REPLAY THE ENTIRE "
                            + "CHANGELOG TOPIC FROM EARLIEST before resuming "
                            + "processing. For a stateful Streams app with "
                            + "non-trivial state (an aggregation table "
                            + "accumulated over weeks, a foreign-key-join "
                            + "materialized view of a 100M-row source), "
                            + "restoration can take 30 minutes to several "
                            + "hours during which the assigned partitions "
                            + "process ZERO input — visible as a multi-hour "
                            + "lag spike on every pod restart. With "
                            + "`num.standby.replicas=1`, a warm copy of "
                            + "every state-store partition is already "
                            + "materialized on a peer instance; failover is "
                            + "sub-second; lag impact is negligible. "
                            + "Specific impacts of the absent key: (a) "
                            + "**aggregation app with 6-month accumulated "
                            + "state** — ~12M state-store keys per partition, "
                            + "restore takes ~25 minutes per pod restart; "
                            + "rolling deploys cause hours of lag-spike "
                            + "windows. (b) **foreign-key-join materialized "
                            + "view** — 100M+ records of materialized state, "
                            + "restore takes 90 minutes per pod; 3-instance "
                            + "rolling deploy takes 4.5 hours of lag "
                            + "degradation. (c) **geographic-DR deployment** "
                            + "— no state replication between AZs; full AZ "
                            + "outage requires surviving AZ to rebuild "
                            + "state from changelog (which itself may be "
                            + "degraded by the outage). (d) **GitOps / "
                            + "immutable infrastructure** — every deploy is "
                            + "a full pod replacement; with "
                            + "`num.standby.replicas=0`, every deploy "
                            + "triggers state-restore cascade across all "
                            + "instances; daily deploys = daily 30-minute "
                            + "degradation windows. Common bug shapes: "
                            + "(1) aggregation app with months of "
                            + "accumulated state — pod restart = 25-min lag "
                            + "spike; (2) foreign-key-join materialized "
                            + "view — rolling deploy = 4.5-hour degradation; "
                            + "(3) geo-DR — AZ outage cascades to changelog "
                            + "replay; (4) GitOps daily-deploy = daily lag "
                            + "windows; (5) Spring Boot Streams autoconfig "
                            + "— default template omits this key, "
                            + "team-wide inheritance of HA-zero posture; "
                            + "(6) Confluent Cloud — operators set `=0` "
                            + "deliberately for bandwidth cost — legitimate "
                            + "but should be EXPLICIT in the .properties. "
                            + "Fix: ONE LINE. Production default: "
                            + "`num.standby.replicas=1` (warm copy of every "
                            + "state-store partition on a peer instance; "
                            + "sub-second failover). Stateless apps: `"
                            + NUM_STANDBY_REPLICAS + "=0` EXPLICITLY to "
                            + "document the deliberate no-state-store "
                            + "choice. Cross-AZ triple-redundancy: `"
                            + NUM_STANDBY_REPLICAS + "=2`. Sibling rule "
                            + "[[streams-num-standby-replicas-zero]] "
                            + "catches the bytecode-detected explicit `=0` "
                            + "in code-side `Properties.put` calls; this "
                            + "rule catches the .properties-file absent "
                            + "case where the operator never set the knob "
                            + "at all. Sibling rule [[streams-properties-"
                            + "replication-factor-absent]] is the durability "
                            + "complement: standbys give you HA on top of "
                            + "the changelog topic's own durability."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
