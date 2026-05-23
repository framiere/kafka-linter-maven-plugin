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
 * {@code probing.rebalance.interval.ms}.
 *
 * <p>Streams declares {@code probing.rebalance.interval.ms} with
 * default {@code 600000} ms (10 minutes), validator
 * {@code ConfigDef.Range.atLeast(60000)} (1 minute minimum), in
 * {@code StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG}. KIP-441
 * (Apache Kafka 2.6, October 2020) introduced this knob as the
 * cadence at which the HighAvailabilityTaskAssignor (default since
 * 2.6) triggers a follow-up rebalance to re-evaluate whether any
 * LAGGED standby has caught up enough to be promoted to active.
 *
 * <p>The default `600000` (10 min) is calibrated for STABLE
 * deployments (rebalances are rare, mostly broker-restart-driven).
 * For HIGH-CHURN deployments (CI-driven rolling deploys, frequent
 * scale-up/scale-down, frequent OOM-restarts), the 10-min cadence
 * is the bottleneck: warm-up replicas catch up in seconds-to-
 * minutes, but the assignor waits the full 10 min before re-
 * evaluating; during the wait, temporary-actives process on stale
 * state, downstream consumers see freshness regressions, and the
 * deployment looks unstable.
 *
 * <p>INFO severity: the default is defensible for stable-deployment
 * workloads but is workload-dependent; the linter cannot infer the
 * deployment's churn rate from .properties alone.
 */
public final class StreamsPropertiesProbingRebalanceIntervalMsAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PROBING_REBALANCE_INTERVAL_MS = "probing.rebalance.interval.ms";

    private final Severity severity;

    public StreamsPropertiesProbingRebalanceIntervalMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_PROBING_REBALANCE_INTERVAL_MS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(PROBING_REBALANCE_INTERVAL_MS))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_PROBING_REBALANCE_INTERVAL_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + PROBING_REBALANCE_INTERVAL_MS, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + PROBING_REBALANCE_INTERVAL_MS + "`. Streams "
                            + "defaults `" + PROBING_REBALANCE_INTERVAL_MS
                            + "` to `600000` (10 minutes, declared in "
                            + "`StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_"
                            + "CONFIG` with `ConfigDef.Type.LONG`, validator "
                            + "`ConfigDef.Range.atLeast(60000)` — 1 minute "
                            + "minimum). KIP-441 (Apache Kafka 2.6, October "
                            + "2020) introduced this knob as the CADENCE at "
                            + "which the HighAvailabilityTaskAssignor "
                            + "(default since 2.6) triggers a follow-up "
                            + "rebalance to RE-CHECK whether any LAGGED "
                            + "standby instance has caught up enough to "
                            + "become ACCEPTABLE (lag <= acceptable.recovery."
                            + "lag). The probing-rebalance flow: (a) initial "
                            + "rebalance picks active assignments preferring "
                            + "the most-caught-up ACCEPTABLE candidate per "
                            + "task; (b) if no ACCEPTABLE candidate exists, "
                            + "the assignor picks the least-lagged LAGGED "
                            + "candidate as a TEMPORARY active and assigns "
                            + "warm-up replicas on other instances; (c) the "
                            + "warm-up replicas catch up in the background by "
                            + "replaying the changelog; (d) every "
                            + PROBING_REBALANCE_INTERVAL_MS + ", the "
                            + "StreamThread leader triggers a follow-up "
                            + "rebalance — if a warm-up replica has caught "
                            + "up, the assignor PROMOTES it to active and "
                            + "demotes the temporary-active; (e) the cycle "
                            + "repeats until all assignments are STABLE. The "
                            + "default `600000` (10 min) is calibrated for "
                            + "STABLE deployments where rebalances are "
                            + "infrequent — the 10-min cadence is "
                            + "unobtrusive. For HIGH-CHURN deployments (CI-"
                            + "driven rolling deploys multiple times per day, "
                            + "frequent scale-up/scale-down events, frequent "
                            + "OOM-restarts), the 10-min cadence is wasteful: "
                            + "warm-up replicas catch up in seconds-to-"
                            + "minutes but the assignor waits the full 10 min "
                            + "before re-evaluating; meanwhile, temporary-"
                            + "actives process on stale state, downstream "
                            + "consumers see freshness regressions, and a "
                            + "rebalance churn can extend a 30-second deploy "
                            + "into a 30-minute stabilization window. "
                            + "Specific bug shapes: (a) **High-churn CI-"
                            + "driven rolling deploys — 10-min default "
                            + "causes prolonged stabilization** — a Streams "
                            + "app is deployed 5-10 times per day; each "
                            + "deploy triggers a rebalance; warm-ups catch "
                            + "up in 30 seconds but the assignor waits 9.5 "
                            + "more minutes before promoting; setting `"
                            + PROBING_REBALANCE_INTERVAL_MS + "=60000` cuts "
                            + "stabilization by 10x. (b) **Frequent scale-up/"
                            + "scale-down via Kubernetes HPA** — autoscaling "
                            + "in response to lag triggers rebalances every "
                            + "few minutes; new pods start as warm-ups; the "
                            + "10-min cadence delays load redistribution; "
                            + "HPA may scale further, exacerbating the issue. "
                            + "(c) **OOM-kill restart with warm-standby** — "
                            + "a pod is OOM-killed; restarted; new pod's "
                            + "warm-ups catch up in 2 minutes; default "
                            + "cadence waits 8 more minutes before "
                            + "promoting. (d) **Stable deployment — default "
                            + "is fine** — manual deploys every few months, "
                            + "rare rebalances; the default 10-min cadence "
                            + "is unobtrusive; the operator might set `"
                            + PROBING_REBALANCE_INTERVAL_MS + "=1800000` (30 "
                            + "min) to reduce coordinator overhead. (e) "
                            + "**Spring Boot Streams autoconfig — org-wide "
                            + "default leaks** — Spring's `KafkaStreams"
                            + "Configuration` bean from `application.yml` "
                            + "omits this key; every Spring Boot Streams app "
                            + "inherits the framework default 600000. (f) "
                            + "**Probing-rebalance churn — 1-min cadence "
                            + "itself becomes the bottleneck** — at "
                            + "`probing.rebalance.interval.ms=60000`, the "
                            + "assignor does 60 evaluations per hour even "
                            + "when no rebalance is needed; for a 100-"
                            + "instance cluster with frequent membership "
                            + "changes, this overhead can saturate the "
                            + "consumer-group coordinator. The right value "
                            + "is workload-dependent — fast enough to react "
                            + "to deployments, slow enough to not churn "
                            + "coordinator state. Fix: ONE LINE. For typical-"
                            + "deployment workloads: `"
                            + PROBING_REBALANCE_INTERVAL_MS + "=600000` "
                            + "(10 min explicit default — documents the "
                            + "deliberate choice). For high-churn "
                            + "deployments: `" + PROBING_REBALANCE_INTERVAL_MS
                            + "=60000` (1 min — minimum allowed; cuts "
                            + "stabilization latency by 10x). For very-"
                            + "stable deployments: `"
                            + PROBING_REBALANCE_INTERVAL_MS + "=1800000` "
                            + "(30 min — reduces coordinator overhead). "
                            + "Sibling rule [[streams-properties-acceptable-"
                            + "recovery-lag-absent]] (the lag-threshold "
                            + "companion — the two knobs work together to "
                            + "define warm-standby promotion behavior)."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
