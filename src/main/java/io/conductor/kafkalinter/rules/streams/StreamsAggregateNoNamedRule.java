package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of any overload of {@code aggregate} on
 * a grouped-stream-like owner ({@link
 * org.apache.kafka.streams.kstream.KGroupedStream KGroupedStream},
 * {@link org.apache.kafka.streams.kstream.KGroupedTable
 * KGroupedTable}, {@link
 * org.apache.kafka.streams.kstream.TimeWindowedKStream
 * TimeWindowedKStream}, {@link
 * org.apache.kafka.streams.kstream.SessionWindowedKStream
 * SessionWindowedKStream}) whose descriptor does NOT include a
 * {@link org.apache.kafka.streams.kstream.Named Named} argument.
 *
 * <p>Predicate is structural — any descriptor for {@code
 * aggregate} on the four grouped owner interfaces whose argument
 * list does NOT contain {@code
 * org/apache/kafka/streams/kstream/Named} is unsafe. That captures
 * the no-Named flavours: {@code aggregate(Initializer,
 * Aggregator)}, {@code aggregate(Initializer, Aggregator,
 * Materialized)}, KGroupedTable's {@code aggregate(Initializer,
 * Aggregator, Aggregator)} and {@code aggregate(Initializer,
 * Aggregator, Aggregator, Materialized)} adder-subtractor
 * variants, plus the session-windowed {@code aggregate(
 * Initializer, Aggregator, Merger)} and {@code aggregate(
 * Initializer, Aggregator, Merger, Materialized)}.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls and {@code
 * INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-Named {@code aggregate()} silently invalidates the
 * aggregate's processor-node identity AND the LARGEST per-key
 * state of any Streams operator on every topology edit</h2>
 *
 * <p>Every Kafka Streams operator compiles into a processor
 * node in the topology graph; every processor node has a name
 * that appears in {@code topology.describe()}, in the JMX MBean
 * tree under {@code kafka.streams:type=stream-processor-node-
 * metrics,processor-node-id=&lt;name&gt;}, and in the internal
 * repartition topic names when the operator is key-changing.
 * When no {@link org.apache.kafka.streams.kstream.Named Named}
 * is passed, Streams synthesizes the processor-node name from
 * the topology graph index — e.g. {@code KSTREAM-AGGREGATE-
 * 0000000007} and (for key-changing upstreams) {@code app-id-
 * KSTREAM-AGGREGATE-0000000007-repartition}.
 *
 * <p>Of the three stateful grouped-stream operators ({@code
 * count}, {@code reduce}, {@code aggregate}), {@code aggregate}
 * carries the LARGEST and MOST EXPENSIVE per-key state — a
 * user-supplied {@code VA} aggregate type that often holds
 * heterogeneous fields (rolling totals, sets, histograms,
 * top-N buffers, latency P99 sketches, etc.). When the
 * processor-node id shifts on a topology edit and (for key-
 * changing upstream) the auto-repartition topic name shifts
 * with it, the cost of losing the JMX MBean and the cost of
 * orphaning the repartition topic are BOTH amplified by the
 * size of the {@code VA} state.
 *
 * <p>{@code aggregate(Initializer, Aggregator, Materialized)}
 * is the SYMMETRIC half-fix to {@code aggregate(Initializer,
 * Aggregator, Named, Materialized)} — Materialized pins the
 * state-store name and the changelog topic, but does NOT touch
 * the processor-node id, the JMX MBean name, or the auto-
 * generated repartition topic. So {@code aggregate(
 * MySession::new, MySession::merge, Materialized.as("session-
 * state"))} fixes the state-store half but leaves the
 * processor-node + JMX + repartition naming graph-index-derived.
 *
 * <p>The combination required for full stability is BOTH {@link
 * org.apache.kafka.streams.kstream.Named Named} (for the
 * processor-node id, JMX MBean, and repartition topic) AND
 * {@link org.apache.kafka.streams.kstream.Materialized
 * Materialized} (for the state store and changelog). This rule
 * covers the Named half; {@code STREAMS_AGGREGATE_NO_MATERIALIZED}
 * covers the Materialized half. Both must be passed for full
 * coverage.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>The aggregate processor-node JMX MBean disappears on
 *       every topology edit.</b> Operations dashboards filter
 *       on {@code processor-node-id="KSTREAM-AGGREGATE-
 *       0000000007"} for the aggregate's process-rate, record-
 *       lateness, and dropped-records metrics. Any upstream
 *       topology edit shifts the graph index; the next deploy
 *       emits MBean {@code KSTREAM-AGGREGATE-0000000008}; the
 *       dashboard reads zero; the operator believes the
 *       aggregate stopped firing. The cost of this gap is
 *       amplified because aggregate dashboards are usually the
 *       PRIMARY business metric — "average order value per
 *       hour", "p99 latency by service", "top-N products" —
 *       not a debug metric.</li>
 *   <li><b>The repartition topic for a key-changing aggregate
 *       rebrands on every topology edit, and the orphan is
 *       larger than count/reduce.</b> Patterns like {@code
 *       stream.selectKey(...).groupByKey().aggregate(...)}
 *       force a key change which causes Streams to create an
 *       internal repartition topic {@code app-id-KSTREAM-
 *       AGGREGATE-0000000007-repartition}. The repartition
 *       topic for aggregate carries the FULL upstream record
 *       (key + value), and the value for aggregate is often
 *       the LARGEST of the three operators (count's value is
 *       Long; reduce's is V; aggregate's is the user VA type
 *       that may pack many fields). After any topology edit
 *       the topic name shifts, the old topic is orphaned on
 *       the broker carrying gigabytes of pre-aggregation
 *       records, and operations sees an unfamiliar topic in
 *       {@code kafka-topics --list} consuming disk it cannot
 *       account for.</li>
 *   <li><b>{@code aggregate(Initializer, Aggregator,
 *       Materialized)} is a half-fix that hides the problem.</b>
 *       A team that named their state store via {@code
 *       aggregate(MySession::new, MySession::merge,
 *       Materialized.as("session-state"))} sees the store name
 *       pinned in {@code topology.describe()} and assumes the
 *       operator is fully named. The processor-node name in
 *       the same topology.describe() is still {@code KSTREAM-
 *       AGGREGATE-0000000007} — graph-index-derived; the JMX
 *       MBean is still graph-index-derived; the auto-
 *       repartition topic (if key-changed upstream) is still
 *       graph-index-derived. The team's monitoring breaks on
 *       the next topology edit even though they "named the
 *       operator".</li>
 *   <li><b>KGroupedTable adder-subtractor aggregate is doubly
 *       affected.</b> KGroupedTable's {@code aggregate(
 *       Initializer, Aggregator, Aggregator)} adder-subtractor
 *       overload (and its {@code (..., Materialized)} half-fix
 *       variant) tracks both a forward and a reverse aggregator
 *       over an upstream KTable, and emits the net effect of
 *       an upstream deletion as a subtraction. The processor
 *       node tracks this paired logic; losing its identity
 *       means both sides are renamed together, and monitoring
 *       on either side breaks on every topology edit. Worse,
 *       a regression in the subtractor (e.g. it doesn't
 *       perfectly invert the adder for the running VA type) is
 *       only detectable by comparing the aggregate's emitted
 *       output against the upstream KTable's net snapshot —
 *       which requires a stable JMX identity to wire into the
 *       comparison job.</li>
 *   <li><b>SessionWindowedKStream merger is silently re-bound
 *       to a graph-index name.</b> {@code aggregate(Initializer,
 *       Aggregator, Merger)} and {@code aggregate(Initializer,
 *       Aggregator, Merger, Materialized)} on
 *       SessionWindowedKStream carry a Merger that joins two
 *       partial sessions when their windows overlap on a late-
 *       arriving record. The merger is invoked from inside the
 *       aggregate's processor node; the merger has no separate
 *       name; if the aggregate's processor-node name shifts,
 *       the merger's metrics (invocation rate, latency) shift
 *       with it. Operations cannot pin a SLO on a session-
 *       window merger without a stable Named on the parent
 *       aggregate.</li>
 *   <li><b>topology.describe() diff is unreadable across
 *       deploys.</b> CI pipelines that diff {@code topology.
 *       describe()} output against a golden file rebuilt on
 *       every PR see spurious renames on every topology edit
 *       — {@code KSTREAM-AGGREGATE-0000000005} → {@code
 *       KSTREAM-AGGREGATE-0000000006} — even when the
 *       aggregate operator itself did not change. The diff is
 *       dominated by graph-index renames and the reviewer
 *       cannot spot the real semantic changes; this is worst
 *       for aggregate because aggregate is the operator most
 *       likely to have business-logic changes (new field added
 *       to the VA type, new edge case in the Aggregator) that
 *       a reviewer needs to focus on.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> An
 *       aggregate factory built as {@code BiFunction&lt;
 *       Initializer&lt;VA&gt;, Aggregator&lt;K, V, VA&gt;,
 *       KTable&lt;K, VA&gt;&gt; a = grouped::aggregate}
 *       compiles to an {@code INVOKEDYNAMIC} site whose bsm-
 *       args contain a {@code REF_invokeInterface} Handle
 *       pointing at {@code KGroupedStream.aggregate(Initializer,
 *       Aggregator)KTable}. The user-class bytecode contains
 *       zero direct {@code INVOKEINTERFACE} on the no-Named
 *       overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named Named} pinning the
 * processor-node id — e.g. {@code grouped.aggregate(MySession::
 * new, MySession::merge, Named.as("session-stats"),
 * Materialized.as("session-stats-store"))}. The explicit name
 * pins the processor-node id, the JMX MBean ({@code processor-
 * node-id=session-stats}), and (for key-changing upstream) the
 * repartition topic ({@code app-id-session-stats-repartition}).
 * For KGroupedTable use the 5-arg overload {@code aggregate(
 * Initializer, Aggregator, Aggregator, Named, Materialized)}.
 */
public final class StreamsAggregateNoNamedRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.GROUPED_KSTREAM_OWNERS;
    private static final String METHOD_NAME = "aggregate";
    private static final String NAMED_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsAggregateNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_AGGREGATE_NO_NAMED;
    }

    private static boolean isUnsafe(String desc) {
        return desc != null && !desc.contains(NAMED_TOKEN);
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && isUnsafe(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, null);
                    if (h != null && isUnsafe(h.getDesc())) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_AGGREGATE_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KGroupedStream / KGroupedTable / TimeWindowed"
                        + "KStream / SessionWindowedKStream . "
                        + "aggregate — a no-Named overload is "
                        + "reached here (aggregate(Initializer, "
                        + "Aggregator), aggregate(Initializer, "
                        + "Aggregator, Materialized), KGrouped"
                        + "Table's aggregate(Initializer, "
                        + "Aggregator, Aggregator) adder-"
                        + "subtractor or its Materialized half-fix, "
                        + "or SessionWindowedKStream's aggregate("
                        + "Initializer, Aggregator, Merger) / "
                        + "(Initializer, Aggregator, Merger, "
                        + "Materialized)) — either as a direct "
                        + "INVOKEINTERFACE on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `grouped::aggregate` bound to a "
                        + "custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). "
                        + "Every Kafka Streams operator compiles "
                        + "into a processor node in the topology "
                        + "graph; every processor node has a name "
                        + "that appears in topology.describe(), in "
                        + "the JMX MBean tree under kafka.streams:"
                        + "type=stream-processor-node-metrics,"
                        + "processor-node-id=<name>, and in the "
                        + "internal repartition topic names when the "
                        + "operator is key-changing. When no Named is "
                        + "passed, Streams synthesizes the processor-"
                        + "node name from the topology graph index "
                        + "(e.g. KSTREAM-AGGREGATE-0000000007 and, "
                        + "for key-changing upstreams, app-id-KSTREAM-"
                        + "AGGREGATE-0000000007-repartition). Of the "
                        + "three stateful grouped-stream operators "
                        + "(count, reduce, aggregate), aggregate "
                        + "carries the LARGEST and MOST EXPENSIVE "
                        + "per-key state — a user-supplied VA "
                        + "aggregate type that often holds "
                        + "heterogeneous fields (rolling totals, "
                        + "sets, histograms, top-N buffers, latency "
                        + "P99 sketches, etc.). aggregate("
                        + "Initializer, Aggregator, Materialized) is "
                        + "the SYMMETRIC half-fix to aggregate("
                        + "Initializer, Aggregator, Named, "
                        + "Materialized) — Materialized pins the "
                        + "state-store name and the changelog topic, "
                        + "but does NOT touch the processor-node id, "
                        + "the JMX MBean name, or the auto-generated "
                        + "repartition topic; so aggregate("
                        + "MySession::new, MySession::merge, "
                        + "Materialized.as(\"session-state\")) fixes "
                        + "the state-store half but leaves the "
                        + "processor-node + JMX + repartition naming "
                        + "graph-index-derived. The combination "
                        + "required for full stability is BOTH Named "
                        + "(for the processor-node id, JMX MBean, "
                        + "and repartition topic) AND Materialized "
                        + "(for the state store and changelog) — "
                        + "pair this rule with STREAMS_AGGREGATE_NO_"
                        + "MATERIALIZED. Concrete failure modes: (1) "
                        + "the aggregate processor-node JMX MBean "
                        + "disappears on every topology edit — "
                        + "operations dashboards filter on processor-"
                        + "node-id=\"KSTREAM-AGGREGATE-0000000007\" "
                        + "for the aggregate's process-rate, record-"
                        + "lateness, and dropped-records metrics; "
                        + "any upstream topology edit shifts the "
                        + "graph index, the next deploy emits MBean "
                        + "KSTREAM-AGGREGATE-0000000008, the "
                        + "dashboard reads zero; the cost is "
                        + "amplified because aggregate dashboards "
                        + "are usually the PRIMARY business metric "
                        + "(\"average order value per hour\", \"p99 "
                        + "latency by service\", \"top-N products\") "
                        + "not a debug metric; (2) the repartition "
                        + "topic for a key-changing aggregate "
                        + "rebrands on every topology edit, and the "
                        + "orphan is LARGER than count/reduce — "
                        + "patterns like stream.selectKey(...)."
                        + "groupByKey().aggregate(...) force a key "
                        + "change which causes Streams to create an "
                        + "internal repartition topic app-id-KSTREAM-"
                        + "AGGREGATE-0000000007-repartition; the "
                        + "repartition topic for aggregate carries "
                        + "the FULL upstream record (key + value), "
                        + "and the value for aggregate is often the "
                        + "LARGEST of the three operators (count's "
                        + "value is Long, reduce's is V, aggregate's "
                        + "is the user VA type that may pack many "
                        + "fields); after any topology edit the "
                        + "topic name shifts, the old topic is "
                        + "orphaned on the broker carrying gigabytes "
                        + "of pre-aggregation records, operations "
                        + "sees an unfamiliar topic in kafka-topics "
                        + "--list consuming disk it cannot account "
                        + "for; (3) aggregate(Initializer, Aggregator, "
                        + "Materialized) is a half-fix that HIDES the "
                        + "problem — a team that named their state "
                        + "store via aggregate(MySession::new, "
                        + "MySession::merge, Materialized.as(\""
                        + "session-state\")) sees the store name "
                        + "pinned in topology.describe() and assumes "
                        + "the operator is fully named; the processor-"
                        + "node name in the same topology.describe() "
                        + "is still KSTREAM-AGGREGATE-0000000007 "
                        + "(graph-index-derived), the JMX MBean is "
                        + "still graph-index-derived, the auto-"
                        + "repartition topic (if key-changed "
                        + "upstream) is still graph-index-derived; "
                        + "the team's monitoring breaks on the next "
                        + "topology edit even though they \"named "
                        + "the operator\"; (4) KGroupedTable adder-"
                        + "subtractor aggregate is doubly affected — "
                        + "KGroupedTable's aggregate(Initializer, "
                        + "Aggregator, Aggregator) adder-subtractor "
                        + "overload (and its (..., Materialized) "
                        + "half-fix variant) tracks both a forward "
                        + "and a reverse aggregator over an upstream "
                        + "KTable and emits the net effect of an "
                        + "upstream deletion as a subtraction; the "
                        + "processor node tracks this paired logic, "
                        + "losing its identity means both sides are "
                        + "renamed together, and monitoring on "
                        + "either side breaks on every topology "
                        + "edit; worse, a regression in the "
                        + "subtractor (e.g. it doesn't perfectly "
                        + "invert the adder for the running VA "
                        + "type) is only detectable by comparing "
                        + "the aggregate's emitted output against "
                        + "the upstream KTable's net snapshot, "
                        + "which requires a stable JMX identity to "
                        + "wire into the comparison job; (5) "
                        + "SessionWindowedKStream merger is "
                        + "silently re-bound to a graph-index name "
                        + "— aggregate(Initializer, Aggregator, "
                        + "Merger) / (Initializer, Aggregator, "
                        + "Merger, Materialized) on Session"
                        + "WindowedKStream carry a Merger that "
                        + "joins two partial sessions when their "
                        + "windows overlap on a late-arriving "
                        + "record; the merger is invoked from "
                        + "inside the aggregate's processor node, "
                        + "the merger has no separate name, if the "
                        + "aggregate's processor-node name shifts "
                        + "the merger's metrics (invocation rate, "
                        + "latency) shift with it, operations "
                        + "cannot pin a SLO on a session-window "
                        + "merger without a stable Named on the "
                        + "parent aggregate; (6) topology.describe() "
                        + "diff is unreadable across deploys — CI "
                        + "pipelines that diff topology.describe() "
                        + "output against a golden file rebuilt on "
                        + "every PR see spurious renames on every "
                        + "topology edit (KSTREAM-AGGREGATE-"
                        + "0000000005 -> KSTREAM-AGGREGATE-"
                        + "0000000006) even when the aggregate "
                        + "operator itself did not change; the diff "
                        + "is dominated by graph-index renames and "
                        + "the reviewer cannot spot the real "
                        + "semantic changes (this is worst for "
                        + "aggregate because aggregate is the "
                        + "operator most likely to have business-"
                        + "logic changes — new field added to the "
                        + "VA type, new edge case in the Aggregator "
                        + "— that a reviewer needs to focus on); "
                        + "(7) INVOKEDYNAMIC method-reference "
                        + "captures bypass naive MethodInsnNode-"
                        + "only lint — `BiFunction<Initializer<VA>, "
                        + "Aggregator<K, V, VA>, KTable<K, VA>> a = "
                        + "grouped::aggregate` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KGroupedStream.aggregate(Initializer, "
                        + "Aggregator)KTable; the user-class "
                        + "bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named "
                        + "overload, only the indy site. Migration: "
                        + "pass an explicit Named pinning the "
                        + "processor-node id — `grouped.aggregate("
                        + "MySession::new, MySession::merge, Named"
                        + ".as(\"session-stats\"), Materialized.as("
                        + "\"session-stats-store\"))`. The explicit "
                        + "name pins the processor-node id, the "
                        + "JMX MBean (processor-node-id=session-"
                        + "stats), and (for key-changing upstream) "
                        + "the repartition topic (app-id-session-"
                        + "stats-repartition). For KGroupedTable "
                        + "use the 5-arg overload aggregate("
                        + "Initializer, Aggregator, Aggregator, "
                        + "Named, Materialized). The aggregate "
                        + "overloads containing Named are never "
                        + "flagged.");
    }
}
