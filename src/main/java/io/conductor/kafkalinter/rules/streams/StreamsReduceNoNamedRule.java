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
 * Fires for every reach of any overload of {@code reduce} on a
 * grouped-stream-like owner ({@link
 * org.apache.kafka.streams.kstream.KGroupedStream KGroupedStream},
 * {@link org.apache.kafka.streams.kstream.KGroupedTable
 * KGroupedTable}, {@link
 * org.apache.kafka.streams.kstream.TimeWindowedKStream
 * TimeWindowedKStream}, {@link
 * org.apache.kafka.streams.kstream.SessionWindowedKStream
 * SessionWindowedKStream}) whose descriptor does NOT include a
 * {@link org.apache.kafka.streams.kstream.Named Named} argument.
 *
 * <p>Predicate is structural — any descriptor for {@code reduce}
 * on the four grouped owner interfaces whose argument list does
 * NOT contain {@code org/apache/kafka/streams/kstream/Named} is
 * unsafe. That captures {@code reduce(Reducer)}, {@code reduce(
 * Reducer, Materialized)}, KGroupedTable's {@code reduce(Reducer,
 * Reducer)} adder-subtractor, and {@code reduce(Reducer, Reducer,
 * Materialized)} but NOT {@code reduce(Reducer, Named,
 * Materialized)} or {@code reduce(Reducer, Reducer, Named,
 * Materialized)}.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls and {@code
 * INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-Named {@code reduce()} silently invalidates the
 * reduce's processor-node identity on every topology edit</h2>
 *
 * <p>Every Kafka Streams operator compiles into a processor
 * node in the topology graph; every processor node has a name
 * that appears in {@code topology.describe()}, in the JMX MBean
 * tree under {@code kafka.streams:type=stream-processor-node-
 * metrics,processor-node-id=&lt;name&gt;}, and in the internal
 * repartition topic names when the operator is key-changing.
 * When no {@link org.apache.kafka.streams.kstream.Named Named}
 * is passed, Streams synthesizes the processor-node name from
 * the topology graph index — e.g. {@code KSTREAM-REDUCE-
 * 0000000007} and (for key-changing upstreams) {@code app-id-
 * KSTREAM-REDUCE-0000000007-repartition}.
 *
 * <p>{@code reduce(Reducer, Materialized)} is the SYMMETRIC
 * half-fix to {@code reduce(Reducer, Named, Materialized)} —
 * Materialized pins the state-store name and the changelog
 * topic, but does NOT touch the processor-node id, the JMX
 * MBean name, or the auto-generated repartition topic. So
 * {@code reduce(Math::max, Materialized.as("max-of-day-store"))}
 * fixes the state-store half but leaves the processor-node +
 * JMX + repartition naming graph-index-derived.
 *
 * <p>The combination required for full stability is BOTH {@link
 * org.apache.kafka.streams.kstream.Named Named} (for the
 * processor-node id, JMX MBean, and repartition topic) AND
 * {@link org.apache.kafka.streams.kstream.Materialized
 * Materialized} (for the state store and changelog). This rule
 * covers the Named half; {@code STREAMS_REDUCE_NO_MATERIALIZED}
 * covers the Materialized half. Both must be passed for full
 * coverage.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>The reduce processor-node JMX MBean disappears on
 *       every topology edit.</b> Operations dashboards filter
 *       on {@code processor-node-id="KSTREAM-REDUCE-
 *       0000000007"} for the reduce's process-rate, record-
 *       lateness, and dropped-records metrics. Any upstream
 *       topology edit shifts the graph index; the next deploy
 *       emits MBean {@code KSTREAM-REDUCE-0000000008}; the
 *       dashboard reads zero; the operator believes the
 *       reduce stopped firing.</li>
 *   <li><b>The repartition topic for a key-changing reduce
 *       rebrands on every topology edit.</b> Patterns like
 *       {@code stream.selectKey(...).groupByKey().reduce(...)}
 *       force a key change which causes Streams to create an
 *       internal repartition topic {@code app-id-KSTREAM-
 *       REDUCE-0000000007-repartition}. After any topology
 *       edit the topic name shifts; the old topic is orphaned
 *       on the broker; operations sees an unfamiliar topic in
 *       {@code kafka-topics --list} and cannot determine which
 *       application owns it.</li>
 *   <li><b>{@code reduce(Reducer, Materialized)} is a half-fix
 *       that hides the problem.</b> A team that named their
 *       state store via {@code reduce(Math::max, Materialized
 *       .as("max-of-day-store"))} sees the store name pinned
 *       in {@code topology.describe()} and assumes the
 *       operator is fully named. The processor-node name in
 *       the same topology.describe() is still {@code KSTREAM-
 *       REDUCE-0000000007} — graph-index-derived; the JMX
 *       MBean is still graph-index-derived; the auto-
 *       repartition topic (if key-changed upstream) is still
 *       graph-index-derived. The team's monitoring breaks on
 *       the next topology edit even though they "named the
 *       operator".</li>
 *   <li><b>KGroupedTable adder-subtractor reduce is doubly
 *       affected.</b> KGroupedTable's {@code reduce(Reducer,
 *       Reducer)} adder-subtractor overload (no Named, no
 *       Materialized) is structurally fragile: it tracks both
 *       a forward and a reverse reducer over an upstream
 *       KTable, and emits the net effect of an upstream
 *       deletion as a subtraction. The processor node tracks
 *       this paired logic; losing its identity means both
 *       sides are renamed together, and monitoring on either
 *       side breaks on every topology edit.</li>
 *   <li><b>topology.describe() diff is unreadable across
 *       deploys.</b> CI pipelines that diff {@code topology.
 *       describe()} output against a golden file rebuilt on
 *       every PR see spurious renames on every topology edit
 *       — {@code KSTREAM-REDUCE-0000000005} → {@code KSTREAM-
 *       REDUCE-0000000006} — even when the reduce operator
 *       itself did not change. The diff is dominated by
 *       graph-index renames and the reviewer cannot spot the
 *       real semantic changes.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       reduce factory built as {@code Function&lt;Reducer&lt;V&gt;,
 *       KTable&lt;K, V&gt;&gt; r = grouped::reduce} compiles to
 *       an {@code INVOKEDYNAMIC} site whose bsm-args contain
 *       a {@code REF_invokeInterface} Handle pointing at
 *       {@code KGroupedStream.reduce(Reducer)KTable}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Named overload, only the
 *       indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named Named} pinning the
 * processor-node id — e.g. {@code grouped.reduce(Math::max,
 * Named.as("max-of-day"), Materialized.as("max-of-day-store"))}.
 * The explicit name pins the processor-node id, the JMX MBean
 * ({@code processor-node-id=max-of-day}), and (for key-changing
 * upstream) the repartition topic ({@code app-id-max-of-day-
 * repartition}). For KGroupedTable use the 4-arg overload
 * {@code reduce(Reducer, Reducer, Named, Materialized)}.
 */
public final class StreamsReduceNoNamedRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.GROUPED_KSTREAM_OWNERS;
    private static final String METHOD_NAME = "reduce";
    private static final String NAMED_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsReduceNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_REDUCE_NO_NAMED;
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
                RuleId.STREAMS_REDUCE_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KGroupedStream / KGroupedTable / TimeWindowed"
                        + "KStream / SessionWindowedKStream . "
                        + "reduce — a no-Named overload is reached "
                        + "here (reduce(Reducer), reduce(Reducer, "
                        + "Materialized), KGroupedTable's reduce("
                        + "Reducer, Reducer) adder-subtractor, or "
                        + "reduce(Reducer, Reducer, Materialized)) — "
                        + "either as a direct INVOKEINTERFACE on the "
                        + "method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `grouped::reduce` "
                        + "bound to a custom SAM whose erased "
                        + "implMethod descriptor matches an unsafe "
                        + "overload). Every Kafka Streams operator "
                        + "compiles into a processor node in the "
                        + "topology graph; every processor node has "
                        + "a name that appears in topology.describe(), "
                        + "in the JMX MBean tree under kafka.streams:"
                        + "type=stream-processor-node-metrics,"
                        + "processor-node-id=<name>, and in the "
                        + "internal repartition topic names when the "
                        + "operator is key-changing. When no Named is "
                        + "passed, Streams synthesizes the processor-"
                        + "node name from the topology graph index "
                        + "(e.g. KSTREAM-REDUCE-0000000007 and, for "
                        + "key-changing upstreams, app-id-KSTREAM-"
                        + "REDUCE-0000000007-repartition). reduce("
                        + "Reducer, Materialized) is the SYMMETRIC "
                        + "half-fix to reduce(Reducer, Named, "
                        + "Materialized) — Materialized pins the "
                        + "state-store name and the changelog topic, "
                        + "but does NOT touch the processor-node id, "
                        + "the JMX MBean name, or the auto-generated "
                        + "repartition topic; so reduce(Math::max, "
                        + "Materialized.as(\"max-of-day-store\")) "
                        + "fixes the state-store half but leaves the "
                        + "processor-node + JMX + repartition naming "
                        + "graph-index-derived. The combination "
                        + "required for full stability is BOTH Named "
                        + "(for the processor-node id, JMX MBean, "
                        + "and repartition topic) AND Materialized "
                        + "(for the state store and changelog) — "
                        + "pair this rule with STREAMS_REDUCE_NO_"
                        + "MATERIALIZED. Concrete failure modes: (1) "
                        + "the reduce processor-node JMX MBean "
                        + "disappears on every topology edit — "
                        + "operations dashboards filter on processor-"
                        + "node-id=\"KSTREAM-REDUCE-0000000007\" for "
                        + "the reduce's process-rate, record-"
                        + "lateness, and dropped-records metrics; "
                        + "any upstream topology edit shifts the "
                        + "graph index; the next deploy emits MBean "
                        + "KSTREAM-REDUCE-0000000008; the dashboard "
                        + "reads zero; the operator believes the "
                        + "reduce stopped firing; (2) the repartition "
                        + "topic for a key-changing reduce rebrands "
                        + "on every topology edit — patterns like "
                        + "stream.selectKey(...).groupByKey().reduce("
                        + "...) force a key change which causes "
                        + "Streams to create an internal repartition "
                        + "topic app-id-KSTREAM-REDUCE-0000000007-"
                        + "repartition; after any topology edit the "
                        + "topic name shifts, the old topic is "
                        + "orphaned on the broker, operations sees "
                        + "an unfamiliar topic in kafka-topics --list "
                        + "and cannot determine which application "
                        + "owns it; (3) reduce(Reducer, Materialized) "
                        + "is a half-fix that HIDES the problem — a "
                        + "team that named their state store via "
                        + "reduce(Math::max, Materialized.as(\"max-"
                        + "of-day-store\")) sees the store name "
                        + "pinned in topology.describe() and assumes "
                        + "the operator is fully named; the "
                        + "processor-node name in the same topology"
                        + ".describe() is still KSTREAM-REDUCE-"
                        + "0000000007 (graph-index-derived), the JMX "
                        + "MBean is still graph-index-derived, the "
                        + "auto-repartition topic (if key-changed "
                        + "upstream) is still graph-index-derived; "
                        + "the team's monitoring breaks on the next "
                        + "topology edit even though they \"named "
                        + "the operator\"; (4) KGroupedTable adder-"
                        + "subtractor reduce is doubly affected — "
                        + "KGroupedTable's reduce(Reducer, Reducer) "
                        + "adder-subtractor overload (no Named, no "
                        + "Materialized) is structurally fragile: it "
                        + "tracks both a forward and a reverse "
                        + "reducer over an upstream KTable and emits "
                        + "the net effect of an upstream deletion as "
                        + "a subtraction; the processor node tracks "
                        + "this paired logic; losing its identity "
                        + "means both sides are renamed together, "
                        + "and monitoring on either side breaks on "
                        + "every topology edit; (5) topology."
                        + "describe() diff is unreadable across "
                        + "deploys — CI pipelines that diff topology"
                        + ".describe() output against a golden file "
                        + "rebuilt on every PR see spurious renames "
                        + "on every topology edit (KSTREAM-REDUCE-"
                        + "0000000005 -> KSTREAM-REDUCE-0000000006) "
                        + "even when the reduce operator itself did "
                        + "not change; the diff is dominated by "
                        + "graph-index renames and the reviewer "
                        + "cannot spot the real semantic changes; "
                        + "(6) INVOKEDYNAMIC method-reference "
                        + "captures bypass naive MethodInsnNode-only "
                        + "lint — `Function<Reducer<V>, KTable<K, V>> "
                        + "r = grouped::reduce` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KGroupedStream.reduce(Reducer)KTable; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit Named pinning the processor-node "
                        + "id — `grouped.reduce(Math::max, Named.as("
                        + "\"max-of-day\"), Materialized.as(\"max-of-"
                        + "day-store\"))`. The explicit name pins "
                        + "the processor-node id, the JMX MBean "
                        + "(processor-node-id=max-of-day), and (for "
                        + "key-changing upstream) the repartition "
                        + "topic (app-id-max-of-day-repartition). "
                        + "For KGroupedTable use the 4-arg overload "
                        + "reduce(Reducer, Reducer, Named, "
                        + "Materialized). The reduce overloads "
                        + "containing Named are never flagged.");
    }
}
