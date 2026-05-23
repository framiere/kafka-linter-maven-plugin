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
 * {@code max.task.idle.ms}.
 *
 * <p>Streams defaults {@code max.task.idle.ms} to {@code 0} (declared
 * in {@code StreamsConfig.MAX_TASK_IDLE_MS_CONFIG},
 * {@code ConfigDef.Type.LONG}). KIP-695 (Apache Kafka 3.0,
 * September 2021) redefined the semantics: {@code 0} means the task
 * NEVER idles waiting for slow input partitions — it processes
 * whichever input partition has data, in wall-clock-arrival order.
 *
 * <p>For TOPOLOGIES WITH JOINS or MERGES (stream-stream, stream-table,
 * table-table, foreign-key-join, {@code merge()}), {@code 0} causes
 * SILENT MISSED JOINS under producer-side jitter or partition
 * rebalances: the task processes the fast side before the slow side
 * delivers matching records, and the join window moves past unmatched
 * records.
 *
 * <p>The recommended production value for join-containing topologies
 * is {@code 100}-{@code 500} ms. The plugin cannot tell whether the
 * topology contains joins from a .properties file alone, but the
 * absence of an explicit choice is overwhelmingly a sign the operator
 * didn't consider join-correctness vs latency trade-offs.
 *
 * <p>INFO severity because the failure mode is silent missed-join
 * data (not crash, not corruption), topologies without joins are
 * unaffected, and the right value is workload-dependent.
 *
 * <p>Complementary to {@code STREAMS_MAX_TASK_IDLE_MS_HIGH} which
 * catches the bytecode-side explicit very-high value (blocking task
 * progress); this rule catches the .properties-file absent case.
 */
public final class StreamsPropertiesMaxTaskIdleMsAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MAX_TASK_IDLE_MS = "max.task.idle.ms";

    private final Severity severity;

    public StreamsPropertiesMaxTaskIdleMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_MAX_TASK_IDLE_MS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(MAX_TASK_IDLE_MS))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_MAX_TASK_IDLE_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + MAX_TASK_IDLE_MS, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + MAX_TASK_IDLE_MS + "`. Streams defaults `"
                            + MAX_TASK_IDLE_MS + "` to `0` — KIP-695 (Apache "
                            + "Kafka 3.0, September 2021) redefined this as "
                            + "'task NEVER idles waiting for slow input "
                            + "partitions'. For topologies WITHOUT multi-input "
                            + "operators (no joins, no merges), `0` is fine — "
                            + "each source has its own task, no cross-input "
                            + "ordering decision to make. For TOPOLOGIES WITH "
                            + "JOINS (stream-stream join, stream-table join, "
                            + "table-table join, foreign-key join) or MERGES "
                            + "(`KStream.merge(other)`), `0` causes SILENT "
                            + "MISSED JOINS: when one side's records arrive "
                            + "10-100 ms slower than the other side (network "
                            + "jitter, producer batching, partition "
                            + "rebalance, restore-from-changelog), the task "
                            + "processes the fast side before the slow side "
                            + "delivers matching records — the join window "
                            + "moves past unmatched records and no join "
                            + "output is emitted for records that SHOULD have "
                            + "matched. The bug is silent (no exception, no "
                            + "metric increment beyond `task-idle-ratio`), "
                            + "non-deterministic (works fine when both sides "
                            + "are balanced, fails under load spikes or "
                            + "rebalances), and hard to diagnose (only "
                            + "noticeable when downstream consumers complain "
                            + "that join output is missing records that "
                            + "'should have matched'). Specific bug shapes: "
                            + "(a) **order-enrichment KStream-KTable join** — "
                            + "orders arrive at 5k/sec, customers KTable "
                            + "updates at 50/sec; with `" + MAX_TASK_IDLE_MS
                            + "=0`, orders process before the local KTable "
                            + "state reflects a customer update that arrived "
                            + "30 ms ago on the brokers; downstream sees "
                            + "occasional enriched orders with stale customer "
                            + "data; setting `" + MAX_TASK_IDLE_MS + "=200` "
                            + "eliminates the stale-state class of bug. (b) "
                            + "**stream-stream window join under producer "
                            + "back-pressure** — `clicks.join(impressions, "
                            + "..., JoinWindows.of(5 minutes))`; impressions "
                            + "producer pauses for 8 seconds during a back-"
                            + "end deploy; clicks task processes at full "
                            + "speed with no impressions buffered; clicks "
                            + "that SHOULD have matched late-arriving "
                            + "impressions are emitted with no join output; "
                            + "join hit rate visibly drops; setting `"
                            + MAX_TASK_IDLE_MS + "=2000` (2 s) absorbs the "
                            + "back-pressure window. (c) **foreign-key join "
                            + "under partition rebalance** — during a "
                            + "rebalance, the right-side state-store is "
                            + "being restored from changelog; for 30 s the "
                            + "right side has no data buffered; left-side "
                            + "task processes left-side updates immediately; "
                            + "the foreign-key-join produces NULL output for "
                            + "records whose right-side match exists but "
                            + "hasn't been restored yet; setting `"
                            + MAX_TASK_IDLE_MS + "=30000` (30 s) absorbs the "
                            + "worst-case restore window. (d) **heterogeneous-"
                            + "rate merge** — `streamA.merge(streamB)` where "
                            + "streamA is 10k/sec and streamB is 10/sec; with "
                            + "`" + MAX_TASK_IDLE_MS + "=0`, merged output is "
                            + "dominated by streamA in wall-clock order, "
                            + "breaking event-time ordering downstream. (e) "
                            + "**Spring Boot Streams autoconfig** — default "
                            + "Spring template omits this knob; every Spring "
                            + "Boot Streams app with joins inherits the "
                            + "default `0`. Fix: ONE LINE. For topologies "
                            + "with joins or merges: `" + MAX_TASK_IDLE_MS
                            + "=200` (200 ms) is a robust production "
                            + "default — bounded latency cost, dramatic "
                            + "improvement in join completeness. For foreign-"
                            + "key joins that must survive partition restore "
                            + "windows: `" + MAX_TASK_IDLE_MS + "=30000` "
                            + "(30 s). For topologies WITHOUT joins (pure-"
                            + "filter, pure-map, single-source-per-task): "
                            + "set explicit `" + MAX_TASK_IDLE_MS + "=0` to "
                            + "document the deliberate choice. Sibling rule "
                            + "[[streams-max-task-idle-ms-high]] catches the "
                            + "bytecode-detected explicit very-high value "
                            + "(blocking task progress); this rule catches "
                            + "the .properties-file absent case where the "
                            + "operator never set the knob at all. Pre-KIP-"
                            + "695 (Apache Kafka <3.0) `" + MAX_TASK_IDLE_MS
                            + "` had different semantics — operators on "
                            + "older versions who set `=0` thinking it meant "
                            + "'use reasonable default' are now on the worst "
                            + "possible setting for join correctness."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
