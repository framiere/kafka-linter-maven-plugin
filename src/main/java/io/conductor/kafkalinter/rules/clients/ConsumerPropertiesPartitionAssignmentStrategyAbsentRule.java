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
 * {@code value.deserializer}) that does NOT set
 * {@code partition.assignment.strategy}.
 *
 * <p>kafka-clients' {@code ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG}
 * has a version-dependent default: 2.x defaulted to {@code [RangeAssignor]}
 * (eager-only); 3.0+ (KIP-726) defaults to
 * {@code [RangeAssignor, CooperativeStickyAssignor]} (negotiates cooperative
 * when all members support it). Leaving the key unset means: silent version-
 * dependent behavior between client versions, one 2.x member silently
 * downgrading a 3.x group to eager rebalance, and the operator never weighing
 * eager-vs-cooperative for their group size.
 *
 * <p>INFO severity because the 3.x default is reasonable for greenfield
 * deployments; the rule is a documentation nudge.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer} OR
 * {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * ({@code connector.class}) and Streams ({@code application.id} — Streams has
 * its own partition assignor based on the topology).
 */
public final class ConsumerPropertiesPartitionAssignmentStrategyAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String PARTITION_ASSIGNMENT_STRATEGY = "partition.assignment.strategy";

    private final Severity severity;

    public ConsumerPropertiesPartitionAssignmentStrategyAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_PARTITION_ASSIGNMENT_STRATEGY_ABSENT;
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
            if (isNonEmpty(p.getProperty(PARTITION_ASSIGNMENT_STRATEGY))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_PARTITION_ASSIGNMENT_STRATEGY_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + PARTITION_ASSIGNMENT_STRATEGY, 0,
                    "kafka-clients consumer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + PARTITION_ASSIGNMENT_STRATEGY + "`. The "
                            + "kafka-clients default is VERSION-DEPENDENT: "
                            + "2.x defaulted to `[RangeAssignor]` (eager-"
                            + "rebalance only); 3.0+ (KIP-726) defaults to "
                            + "`[RangeAssignor, CooperativeStickyAssignor]` "
                            + "(negotiates CooperativeSticky when ALL group "
                            + "members support it, otherwise falls back to "
                            + "RangeAssignor). The eager protocol (Range, "
                            + "RoundRobin, Sticky) revokes ALL partitions "
                            + "from ALL members at the start of EVERY "
                            + "rebalance — during the revoke→reassign window, "
                            + "NO consumer in the group is processing. The "
                            + "cooperative protocol (CooperativeStickyAssignor, "
                            + "KIP-429) only revokes the partitions that "
                            + "actually need to move; consumers that keep "
                            + "their existing partitions continue processing. "
                            + "For a 20-instance group reading 200 partitions, "
                            + "this is the difference between 1-3 seconds of "
                            + "GROUP-WIDE stop-the-world per rebalance vs "
                            + "~50ms (only the partitions that move). "
                            + "Specific impacts of the absent key: (a) "
                            + "**Silent version-coupled behavior** — the "
                            + "same .properties on kafka-clients 2.x runs "
                            + "eager Range, on 3.x runs cooperative when "
                            + "everyone supports it; a client-version "
                            + "upgrade silently changes rebalance behavior. "
                            + "(b) **Mixed-version-group downgrade** — one "
                            + "2.x worker in a 3.x group ends up forcing "
                            + "RangeAssignor on EVERYONE (group coordinator "
                            + "picks the intersection of advertised "
                            + "strategies). (c) **No explicit trade-off**  — "
                            + "operator never weighs eager-vs-cooperative "
                            + "for their group size. Common bug shapes: "
                            + "(1) 20-instance group with 1000 partitions "
                            + "stalls 4s per rebalance under default Range; "
                            + "switching to CooperativeStickyAssignor "
                            + "explicitly reduces stall to ~50ms; (2) "
                            + "2.x→3.x upgrade silently 'improves' rebalance "
                            + "behavior; team takes credit without "
                            + "understanding why; later 3.x→2.x rollback "
                            + "silently regresses, team cannot reproduce; "
                            + "(3) one 2.x ETL worker in a 3.x consumer "
                            + "group downgrades the whole group to eager; "
                            + "SLO violations during deploys are blamed on "
                            + "the 3.x client; (4) Spring Boot apps inherit "
                            + "whatever the bundled kafka-clients version "
                            + "defaults to. Fix: ONE LINE. Set `"
                            + PARTITION_ASSIGNMENT_STRATEGY
                            + "=org.apache.kafka.clients.consumer."
                            + "CooperativeStickyAssignor` for homogeneous "
                            + "3.x+ fleets (best rebalance behavior); or "
                            + "`org.apache.kafka.clients.consumer."
                            + "RangeAssignor,org.apache.kafka.clients."
                            + "consumer.CooperativeStickyAssignor` for "
                            + "rolling-upgrade scenarios (matches 3.x "
                            + "default explicitly, allows migration); or "
                            + "`org.apache.kafka.clients.consumer."
                            + "RangeAssignor` EXPLICITLY to document a "
                            + "deliberate choice of eager-rebalance. "
                            + "Sibling rule [[consumer-partition-assignment-"
                            + "legacy]] catches the explicit-pinned-to-"
                            + "legacy case (Range/RoundRobin/Sticky without "
                            + "CooperativeSticky in the list)."));
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
