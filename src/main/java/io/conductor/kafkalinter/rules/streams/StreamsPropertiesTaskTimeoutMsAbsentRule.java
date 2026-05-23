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
 * {@code task.timeout.ms}.
 *
 * <p>Streams declares {@code task.timeout.ms} with default {@code 300000}
 * (5 minutes) in {@code StreamsConfig.TASK_TIMEOUT_MS_CONFIG}. KIP-572
 * (Apache Kafka 2.8, June 2021) introduced this knob as the UNIFIED
 * retry budget for all retriable exceptions encountered by a
 * StreamThread during task processing: {@code TimeoutException} from
 * producer/consumer, {@code OffsetOutOfRangeException},
 * {@code TaskCorruptedException}, transient leadership-change
 * exceptions, etc. When the cumulative retry time on a single task
 * exceeds {@code task.timeout.ms}, the task transitions to FAILED.
 *
 * <p>The default 5 min covers typical rolling-restart outages but
 * does NOT cover prolonged events (controller failover, AZ isolation,
 * cluster-wide upgrades — all 10-30 min envelopes), and does NOT
 * match the intent of fail-fast workloads (financial settlement,
 * regulatory CDC — should page on the FIRST retriable error rather
 * than retry for 5 min).
 *
 * <p>INFO severity because the default is defensible for typical
 * workloads but is workload-dependent; the linter cannot infer the
 * workload character from .properties alone.
 *
 * <p>Sibling rules: {@code STREAMS_TASK_TIMEOUT_MS_TOO_HIGH} and
 * {@code STREAMS_TASK_TIMEOUT_MS_ZERO} catch bytecode-side overt
 * mis-configuration in {@code Properties.put()} calls; this rule
 * catches the .properties-file absent case.
 */
public final class StreamsPropertiesTaskTimeoutMsAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TASK_TIMEOUT_MS = "task.timeout.ms";

    private final Severity severity;

    public StreamsPropertiesTaskTimeoutMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_TASK_TIMEOUT_MS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(TASK_TIMEOUT_MS))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_TASK_TIMEOUT_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + TASK_TIMEOUT_MS, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + TASK_TIMEOUT_MS + "`. Streams defaults `"
                            + TASK_TIMEOUT_MS + "` to `300000` (5 minutes, "
                            + "declared in `StreamsConfig.TASK_TIMEOUT_MS_"
                            + "CONFIG` with `ConfigDef.Type.LONG`). KIP-572 "
                            + "(Apache Kafka 2.8, June 2021) introduced this "
                            + "knob as the UNIFIED retry budget for ALL "
                            + "retriable exceptions a StreamThread encounters "
                            + "during task processing: `TimeoutException` from "
                            + "the embedded producer's send-callback when a "
                            + "broker is briefly unavailable; `TimeoutException` "
                            + "from the embedded consumer's poll / commit / "
                            + "listOffsets; `OffsetOutOfRangeException` (the "
                            + "StreamThread's position is no longer on the "
                            + "broker — recoverable via the configured "
                            + "`auto.offset.reset`); `TaskCorruptedException` "
                            + "(state-store / changelog inconsistency — "
                            + "recoverable via a state-restore round-trip); "
                            + "transient `NotEnoughReplicasException`, "
                            + "`NotLeaderForPartitionException`, "
                            + "`LeaderNotAvailableException`, "
                            + "`UnknownTopicOrPartitionException` from a broker "
                            + "mid-leadership-change. The StreamThread "
                            + "accumulates retry attempts and tracks the "
                            + "cumulative wall-clock time spent retrying ONE "
                            + "task; if that cumulative time exceeds `"
                            + TASK_TIMEOUT_MS + "`, the task transitions to "
                            + "FAILED and the configured `Thread.Uncaught"
                            + "ExceptionHandler` (default `SHUTDOWN_CLIENT`) "
                            + "decides the fate of the whole KafkaStreams "
                            + "instance. The default `300000` (5 min) covers "
                            + "typical rolling-restart outages but does NOT "
                            + "cover two important failure shapes. Specific "
                            + "bug shapes: (a) **Controller failover with "
                            + "metadata-quorum recovery — 5 min not enough** — "
                            + "KRaft controller failure triggers a metadata-"
                            + "quorum re-election; recovery takes 8 min; at "
                            + "min 5 the StreamThread's task budget is "
                            + "exhausted and the topology dies; Kubernetes "
                            + "restarts the pod into the same outage stream "
                            + "and the new pod dies at min 10 — CrashLoop"
                            + "BackOff until the cluster fully recovers; "
                            + "setting `" + TASK_TIMEOUT_MS + "=1800000` "
                            + "(30 min) absorbs the worst-case window. (b) "
                            + "**Network-partition between Streams instance "
                            + "AZ and broker AZ — 5 min not enough** — an "
                            + "AZ-isolation event isolates the Streams pod's "
                            + "AZ from the broker AZ for 12 min; every send "
                            + "and poll times out; at min 5 the task budget "
                            + "is exhausted and the topology dies; after the "
                            + "partition heals the new pod recovers but lost "
                            + "12 min of processing time; setting `"
                            + TASK_TIMEOUT_MS + "=1800000` (30 min) covers "
                            + "the partition envelope. (c) **Cluster-wide "
                            + "config change with 10-min convergence — 5 min "
                            + "not enough** — operator pushes a cluster-wide "
                            + "`inter.broker.protocol.version` upgrade; 30+ "
                            + "brokers roll-upgrade over 10 min; Streams "
                            + "instances see a stream of "
                            + "`NotLeaderForPartitionException`, "
                            + "`LeaderNotAvailableException`, "
                            + "`TimeoutException` mixed throughout; the "
                            + "task budget exhausts at the halfway point and "
                            + "the topology dies; setting `" + TASK_TIMEOUT_MS
                            + "=1800000` (30 min) covers the upgrade window. "
                            + "(d) **Fail-fast financial settlement — 5 min "
                            + "default MASKS a real bug** — operator "
                            + "deliberately wants retriable errors to halt "
                            + "the topology and page on-call IMMEDIATELY; "
                            + "with the default 5-min budget the on-call is "
                            + "paged 5 min after the first error rather than "
                            + "seconds; setting `" + TASK_TIMEOUT_MS + "=0` "
                            + "opts out of retries entirely and surfaces the "
                            + "error immediately. (e) **Spring Boot Streams "
                            + "autoconfig — org-wide default leaks through** "
                            + "— Spring Boot's Streams autoconfiguration "
                            + "provides a `KafkaStreamsConfiguration` bean "
                            + "from `application.yml`; the default Spring "
                            + "template omits `" + TASK_TIMEOUT_MS + "`; "
                            + "every Spring Boot Streams app inherits the "
                            + "framework default 5 min; an org with diverse "
                            + "workloads (some need 30-min tolerance, some "
                            + "need fail-fast) cannot tune per-app via the "
                            + "default-leak path. (f) **Streams version "
                            + "upgrade — pre-KIP-572 behavior was different** "
                            + "— Apache Kafka <2.8 had per-exception retry "
                            + "handling; upgrading to 2.8+ silently "
                            + "introduces the 5-min cumulative budget; the "
                            + "same workload that survived a 10-min broker "
                            + "outage on 2.7 now dies at the 5-min mark on "
                            + "2.8+; the bug appears the next time there's a "
                            + "long-tail outage. Fix: ONE LINE. For typical "
                            + "production workloads: `" + TASK_TIMEOUT_MS
                            + "=300000` (5 min — explicit; documents the "
                            + "deliberate choice; insulates from future "
                            + "framework default changes). For workloads "
                            + "that must survive prolonged broker outages "
                            + "(controller failover, AZ isolation, cluster-"
                            + "wide upgrades): `" + TASK_TIMEOUT_MS
                            + "=1800000` (30 min). For fail-fast workloads "
                            + "(financial settlement, regulatory CDC, "
                            + "ledger writes): `" + TASK_TIMEOUT_MS + "=0` "
                            + "(opts out of retries — pages on-call on the "
                            + "first retriable error). Sibling rules "
                            + "[[streams-task-timeout-ms-too-high]] and "
                            + "[[streams-task-timeout-ms-zero]] catch overt-"
                            + "form mis-configuration in code-side "
                            + "`Properties.put()` calls; this rule catches "
                            + "the .properties-file absent case where the "
                            + "operator never set the knob at all."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
