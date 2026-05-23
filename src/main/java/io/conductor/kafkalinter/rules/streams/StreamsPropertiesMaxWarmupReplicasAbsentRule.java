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
 * {@code max.warmup.replicas}.
 *
 * <p>Streams declares {@code max.warmup.replicas} with default
 * {@code 2}, validator {@code ConfigDef.Range.atLeast(1)}, in
 * {@code StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG}. KIP-441 (Apache
 * Kafka 2.6, October 2020) introduced this knob as the cluster-wide
 * BUDGET on how many concurrent warm-up standby tasks the
 * HighAvailabilityTaskAssignor (default since 2.6) can have in-
 * flight per probing-rebalance round.
 *
 * <p>The default `2` is calibrated for SMALL-TO-MEDIUM clusters (2-8
 * instances, 10-100 tasks). For LARGE clusters (20+ instances,
 * 200+ tasks) or HIGH-CHURN environments (frequent scale events,
 * frequent rolling deploys), the default `2` is the BOTTLENECK:
 * stabilizing a 50-task reassignment after a major rebalance event
 * takes 25 probing-rebalance rounds — many hours at the default
 * 10-min cadence.
 *
 * <p>INFO severity: the default is defensible for small-to-medium
 * clusters but is workload-dependent; the linter cannot infer
 * cluster size or task count from .properties alone.
 */
public final class StreamsPropertiesMaxWarmupReplicasAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MAX_WARMUP_REPLICAS = "max.warmup.replicas";

    private final Severity severity;

    public StreamsPropertiesMaxWarmupReplicasAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_MAX_WARMUP_REPLICAS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(MAX_WARMUP_REPLICAS))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_MAX_WARMUP_REPLICAS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + MAX_WARMUP_REPLICAS, 0,
                    "Kafka Streams .properties file (detected via top-"
                            + "level `" + APPLICATION_ID + "`) does NOT set `"
                            + MAX_WARMUP_REPLICAS + "`. Streams defaults `"
                            + MAX_WARMUP_REPLICAS + "` to `2` (declared in "
                            + "`StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG` "
                            + "with `ConfigDef.Type.INT`, validator "
                            + "`ConfigDef.Range.atLeast(1)`). KIP-441 "
                            + "(Apache Kafka 2.6, October 2020) introduced "
                            + "this knob as the cluster-wide BUDGET on how "
                            + "many concurrent WARM-UP standby tasks the "
                            + "HighAvailabilityTaskAssignor (default since "
                            + "2.6) can have in-flight per probing-"
                            + "rebalance round. A 'warm-up replica' is a "
                            + "TRANSIENT standby task the assignor spins up "
                            + "when it wants to promote a task to a new "
                            + "instance that doesn't yet have an ACCEPTABLE "
                            + "copy of the state — distinct from a "
                            + "`num.standby.replicas`-driven STEADY-STATE "
                            + "standby. Each warm-up adds load: (a) one "
                            + "extra consumer-group member on the changelog "
                            + "partition (broker fetch load); (b) state-"
                            + "store disk-space on the destination instance "
                            + "(RocksDB; potentially GB); (c) one StreamThread "
                            + "slot occupied during the warm-up. The default "
                            + "`2` is calibrated for SMALL-TO-MEDIUM clusters "
                            + "(2-8 instances, 10-100 tasks total) where 2 "
                            + "concurrent warm-ups per round is enough "
                            + "progress per probing-rebalance cycle. For "
                            + "LARGE clusters (20+ instances, 200+ tasks) or "
                            + "HIGH-CHURN environments (frequent scaling "
                            + "events, frequent rolling deploys), the "
                            + "default `2` is the BOTTLENECK: stabilizing a "
                            + "50-task reassignment after a major rebalance "
                            + "event takes 25 probing-rebalance rounds × "
                            + "probing.rebalance.interval.ms — at the "
                            + "default 10-min cadence, this is OVER 4 HOURS "
                            + "of unstable assignment during which the "
                            + "assignor uses temporary-active candidates on "
                            + "stale state. Specific bug shapes: (a) **Large "
                            + "cluster post-major-rebalance — stabilization "
                            + "takes hours** — a 20-instance cluster with "
                            + "200 tasks; a 5-instance batch restart "
                            + "triggers ~50-task reassignment; default `2` "
                            + "+ default `probing.rebalance.interval.ms=600000` "
                            + "= 4+ hours stabilization; setting `"
                            + MAX_WARMUP_REPLICAS + "=10` + `probing."
                            + "rebalance.interval.ms=60000` drops it to 5 "
                            + "min. (b) **High-churn HPA-driven autoscaling** "
                            + "— HPA scales 4→16 instances over 30 min; "
                            + "each scale event triggers warm-ups; default "
                            + "`2` cluster-wide cap means each event takes "
                            + "6+ rounds to stabilize; HPA cascade leaves "
                            + "cluster in perpetual probing-rebalance. (c) "
                            + "**Memory-constrained instances — warm-ups OOM** "
                            + "— 10 GiB state-stores; two simultaneous warm-"
                            + "ups on same instance can OOM the JVM "
                            + "(doubling disk/memory transiently); setting "
                            + MAX_WARMUP_REPLICAS + "=1` serializes warm-up "
                            + "I/O to avoid double-store overhead. (d) "
                            + "**Small cluster (2-8 instances) — default is "
                            + "fine** — a 4-instance, 20-task cluster "
                            + "stabilizes in 2-3 rounds at default. (e) "
                            + "**Spring Boot Streams autoconfig — org-wide "
                            + "default leaks** — Spring's `KafkaStreams"
                            + "Configuration` bean omits this key; every "
                            + "Spring Boot Streams app inherits the "
                            + "framework default 2. (f) **Cluster expansion "
                            + "— promoting 100 new tasks takes hours** — "
                            + "scaling 10→30 instances after a traffic "
                            + "forecast surge; 20 new instances start "
                            + "empty; default cap means 100-task expansion "
                            + "takes 50 rounds × 10 min = 8+ hours of over-"
                            + "load on the 10 existing instances. Fix: ONE "
                            + "LINE. For small/medium clusters (2-8 "
                            + "instances): `" + MAX_WARMUP_REPLICAS + "=2` "
                            + "(explicit default — documents the deliberate "
                            + "choice). For large clusters (20+ instances): "
                            + "`" + MAX_WARMUP_REPLICAS + "=10` (proportional "
                            + "to fleet size — allows fast stabilization). "
                            + "For memory-constrained workloads: `"
                            + MAX_WARMUP_REPLICAS + "=1` (minimum allowed; "
                            + "serializes warm-up I/O to avoid double-"
                            + "state-store overhead). Sibling rules "
                            + "[[streams-properties-acceptable-recovery-lag-"
                            + "absent]] (the lag-threshold companion) and "
                            + "[[streams-properties-probing-rebalance-"
                            + "interval-ms-absent]] (the probing-cadence "
                            + "companion) — the three knobs together "
                            + "define warm-standby promotion behavior."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
