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
 * {@code acceptable.recovery.lag}.
 *
 * <p>Streams declares {@code acceptable.recovery.lag} with default
 * {@code 10000} records in
 * {@code StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG}. KIP-441
 * (Apache Kafka 2.6, October 2020) introduced this knob as the
 * threshold of changelog-record lag below which a standby task is
 * considered 'caught up enough' to be promoted to active during a
 * rebalance.
 *
 * <p>The default {@code 10000} is calibrated for typical-throughput
 * workloads (5k-50k rec/sec/partition, where 10k records is sub-second
 * of lag). For LOW-throughput workloads (&lt;100 rec/sec/partition),
 * 10,000 records may represent minutes-to-hours of stale state — a
 * promoted standby could be that far behind in event-time.
 *
 * <p>INFO severity: the default is defensible for typical-throughput
 * workloads but is workload-dependent; the linter cannot infer the
 * topology's throughput from .properties alone.
 */
public final class StreamsPropertiesAcceptableRecoveryLagAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String ACCEPTABLE_RECOVERY_LAG = "acceptable.recovery.lag";

    private final Severity severity;

    public StreamsPropertiesAcceptableRecoveryLagAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_ACCEPTABLE_RECOVERY_LAG_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(ACCEPTABLE_RECOVERY_LAG))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_ACCEPTABLE_RECOVERY_LAG_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + ACCEPTABLE_RECOVERY_LAG, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + ACCEPTABLE_RECOVERY_LAG + "`. Streams defaults `"
                            + ACCEPTABLE_RECOVERY_LAG + "` to `10000` "
                            + "(10,000 records, declared in `StreamsConfig."
                            + "ACCEPTABLE_RECOVERY_LAG_CONFIG` with "
                            + "`ConfigDef.Type.LONG`). KIP-441 (Apache Kafka "
                            + "2.6, October 2020) introduced this knob as "
                            + "the WARM-STANDBY PROMOTION THRESHOLD: during "
                            + "a rebalance, a standby task with changelog-"
                            + "lag <= acceptable.recovery.lag is eligible "
                            + "for promotion to active. The "
                            + "HighAvailabilityTaskAssignor (default since "
                            + "2.6) prefers the most-caught-up ACCEPTABLE "
                            + "candidate; if no ACCEPTABLE candidate exists, "
                            + "falls back to the least-lagged LAGGED "
                            + "candidate and triggers a probing rebalance "
                            + "(every `probing.rebalance.interval.ms`, "
                            + "default 10 min) until an ACCEPTABLE "
                            + "candidate emerges. The default `10000` is "
                            + "calibrated for typical-throughput workloads "
                            + "(5k-50k rec/sec/partition, where 10k records "
                            + "is sub-second of lag), but is workload-"
                            + "dependent at the extremes. Specific bug "
                            + "shapes: (a) **Low-throughput CDC stream — "
                            + "standby is hours behind but considered "
                            + "acceptable** — a Streams app aggregates a "
                            + "low-volume event stream (e.g., 10 events/"
                            + "sec); the default `acceptable.recovery.lag="
                            + "10000` means a standby is acceptable when "
                            + "its lag is 10,000 records or less; at 10 "
                            + "events/sec, 10,000 records = ~17 minutes; a "
                            + "rebalance promotes a standby that's 17 "
                            + "minutes behind in event-time; the new active "
                            + "emits a flood of catch-up aggregates; "
                            + "downstream alerting fires 'data freshness "
                            + "regression' alarms; the operator scrambles "
                            + "to understand why a rebalance caused a "
                            + "17-minute staleness blip. Setting `"
                            + ACCEPTABLE_RECOVERY_LAG + "=100` ensures "
                            + "promotion only when the standby is within "
                            + "100 records (= 10 seconds at 10 events/sec) "
                            + "of the active. (b) **Very high-throughput "
                            + "stream — default is fine but a more "
                            + "aggressive setting cuts probing rebalances** "
                            + "— a Streams app at 500k records/sec/"
                            + "partition: 10,000 records of lag is 20 ms; "
                            + "the default is fine but the operator could "
                            + "set `" + ACCEPTABLE_RECOVERY_LAG + "=100000` "
                            + "(100k = 200 ms) to reduce probing-rebalance "
                            + "frequency during cold-start scenarios. (c) "
                            + "**Sparse-update KTable with compacted source** "
                            + "— a Streams app builds a KTable from a "
                            + "compacted topic (customer-profile changes, "
                            + "~1 event/key/day); the default could be "
                            + "exceeded only during the initial state-"
                            + "restore; the knob is effectively inert. (d) "
                            + "**Bursty workload — calm period vs spike** — "
                            + "a Streams app at 100 events/sec average and "
                            + "50,000 events/sec spike; during calm, 10,000 "
                            + "records = 100 seconds of state; during "
                            + "spike, 10,000 records = 200 ms; the right "
                            + "value depends on which mode the operator "
                            + "optimizes for. (e) **Cross-cluster "
                            + "MirrorMaker2-mirrored source** — the standby "
                            + "lag is measured against the local changelog, "
                            + "not against the original source; "
                            + "MirrorMaker2 lag doesn't directly affect "
                            + "this knob, but the operator should be aware "
                            + "that 'standby is acceptable' doesn't mean "
                            + "'standby is current with the source DB'. (f) "
                            + "**Spring Boot Streams autoconfig — org-wide "
                            + "default leaks through** — Spring Boot's "
                            + "Streams autoconfiguration provides a "
                            + "`KafkaStreamsConfiguration` bean from "
                            + "`application.yml`; the default Spring "
                            + "template omits `" + ACCEPTABLE_RECOVERY_LAG
                            + "`; every Spring Boot Streams app inherits "
                            + "the framework default 10,000. Fix: ONE "
                            + "LINE. For typical-throughput workloads: `"
                            + ACCEPTABLE_RECOVERY_LAG + "=10000` (explicit "
                            + "default — documents the deliberate choice). "
                            + "For very-high-throughput workloads: `"
                            + ACCEPTABLE_RECOVERY_LAG + "=100000` (100k "
                            + "records is still <1 second at 200k rec/sec). "
                            + "For LOW-throughput workloads: `"
                            + ACCEPTABLE_RECOVERY_LAG + "=100` (small "
                            + "absolute lag — promotes only when standby is "
                            + "genuinely caught up). For fail-fast / no-"
                            + "tolerance workloads: `" + ACCEPTABLE_RECOVERY_LAG
                            + "=0` (only promote when EXACTLY caught up — "
                            + "eliminates any state-staleness on promotion). "
                            + "Sibling rules [[streams-properties-num-standby-"
                            + "replicas-absent]] (the warm-standby companion "
                            + "— declares how many warm copies exist) and "
                            + "[[streams-properties-probing-rebalance-"
                            + "interval-ms-absent]] (the probing-rebalance "
                            + "pacing companion)."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
