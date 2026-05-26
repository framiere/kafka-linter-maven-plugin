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
 * Fires for every reach of any overload of {@code count} on a
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
 * <p>Predicate is structural — any descriptor for {@code count}
 * on the four grouped owner interfaces whose argument list does
 * NOT contain {@code org/apache/kafka/streams/kstream/Named} is
 * unsafe. That captures both {@code count()} and {@code
 * count(Materialized)} but NOT {@code count(Named)} or {@code
 * count(Named, Materialized)}.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (all
 * four owners are interfaces) and {@code INVOKEDYNAMIC} method-
 * reference captures (e.g. {@code stream::count} bound to {@link
 * java.util.function.Supplier Supplier&lt;KTable&gt;}, to {@link
 * java.util.function.Function Function&lt;Materialized, KTable&gt;},
 * or to a custom SAM whose erased implMethod descriptor matches
 * an unsafe overload).
 *
 * <h2>Why no-Named {@code count()} silently invalidates the
 * count's processor-node identity on every topology edit</h2>
 *
 * <p>Every Kafka Streams operator compiles into a processor
 * node in the topology graph; every processor node has a name
 * that appears in {@code topology.describe()}, in the JMX
 * MBean tree under {@code kafka.streams:type=stream-processor-
 * node-metrics,processor-node-id=&lt;name&gt;}, and in the
 * internal repartition topic names when the operator is
 * key-changing. When no {@link
 * org.apache.kafka.streams.kstream.Named Named} is passed,
 * Streams synthesizes the processor-node name from the graph
 * index — e.g. {@code KSTREAM-AGGREGATE-0000000007},
 * {@code KSTREAM-AGGREGATE-STATE-STORE-0000000007}, and (for
 * key-changing upstreams) {@code app-id-KSTREAM-AGGREGATE-
 * 0000000007-repartition}.
 *
 * <p>{@code count(Materialized)} is the SYMMETRIC half-fix to
 * {@code count(Named)} — Materialized pins the state-store
 * name and the changelog topic, but does NOT touch the
 * processor-node id, the JMX MBean name, or the auto-generated
 * repartition topic. So {@code count(Materialized.as("dau-
 * counter-store"))} fixes the state-store half but leaves the
 * processor-node + JMX + repartition naming graph-index-derived.
 *
 * <p>The combination required for full stability is BOTH {@link
 * org.apache.kafka.streams.kstream.Named Named} (for the
 * processor-node id, JMX MBean, and repartition topic) AND
 * {@link org.apache.kafka.streams.kstream.Materialized
 * Materialized} (for the state store and changelog). This rule
 * covers the Named half; {@code STREAMS_COUNT_NO_MATERIALIZED}
 * covers the Materialized half. Both must be passed for full
 * coverage.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>The processor-node JMX MBean disappears on every
 *       topology edit.</b> Operations dashboards filter on
 *       {@code processor-node-id="KSTREAM-AGGREGATE-
 *       0000000007"}. Any upstream topology edit shifts the
 *       graph index; the next deploy emits MBean {@code
 *       KSTREAM-AGGREGATE-0000000008}; the dashboard reads
 *       zero for the count's process-rate, record-lateness,
 *       and dropped-records metrics; the operator believes
 *       the counter is broken.</li>
 *   <li><b>The repartition topic for a key-changing upstream
 *       rebrands on every topology edit.</b> Patterns like
 *       {@code stream.selectKey(...).groupByKey().count()}
 *       force a key change, which causes Streams to create an
 *       internal repartition topic {@code app-id-KSTREAM-
 *       AGGREGATE-0000000007-repartition}. After any topology
 *       edit the topic name shifts to {@code -0000000008-
 *       repartition}; the old topic is orphaned on the broker
 *       (Streams does not auto-delete it); operations sees an
 *       unfamiliar topic in {@code kafka-topics --list} and
 *       cannot determine which application owns it.</li>
 *   <li><b>{@code count(Materialized)} is a half-fix that
 *       hides the problem.</b> A team that named their state
 *       store via {@code count(Materialized.as("dau-counter-
 *       store"))} sees the store name pinned in {@code
 *       topology.describe()} and assumes the operator is
 *       fully named. The processor-node name in the same
 *       topology.describe() is still {@code KSTREAM-AGGREGATE-
 *       0000000007} — graph-index-derived; the JMX MBean is
 *       still graph-index-derived; the auto-repartition topic
 *       (if key-changed upstream) is still graph-index-
 *       derived. The team's monitoring breaks on the next
 *       topology edit even though they "named the operator".</li>
 *   <li><b>topology.describe() diff is unreadable across
 *       deploys.</b> CI pipelines that diff {@code topology.
 *       describe()} output against a golden file rebuilt on
 *       every PR see spurious renames on every topology edit
 *       — {@code KSTREAM-AGGREGATE-0000000005} → {@code
 *       KSTREAM-AGGREGATE-0000000006} — even when the count
 *       operator itself did not change. The diff is dominated
 *       by graph-index renames and the reviewer cannot spot
 *       the real semantic changes.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       count factory built as {@code Supplier&lt;KTable&lt;K,
 *       Long&gt;&gt; counter = grouped::count} compiles to an
 *       {@code INVOKEDYNAMIC} site whose bsm-args contain a
 *       {@code REF_invokeInterface} Handle pointing at {@code
 *       KGroupedStream.count()KTable}. The user-class bytecode
 *       contains zero direct {@code INVOKEINTERFACE} on the
 *       no-Named overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named Named} pinning the
 * processor-node id — e.g. {@code grouped.count(Named.as(
 * "dau-counter"))}. The explicit name pins the processor-node
 * id, the JMX MBean ({@code processor-node-id=dau-counter}),
 * and (for key-changing upstream) the repartition topic
 * ({@code app-id-dau-counter-repartition}). For full naming
 * coverage, pair {@code Named.as(...)} with {@code
 * Materialized.as(...)} (the 4-arg {@code count(Named,
 * Materialized)} overload).
 */
public final class StreamsCountNoNamedRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.GROUPED_KSTREAM_OWNERS;
    private static final String METHOD_NAME = "count";
    private static final String NAMED_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsCountNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_COUNT_NO_NAMED;
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
                RuleId.STREAMS_COUNT_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KGroupedStream / KGroupedTable / TimeWindowed"
                        + "KStream / SessionWindowedKStream . count "
                        + "— a no-Named overload is reached here "
                        + "(either count() or count(Materialized)) — "
                        + "either as a direct INVOKEINTERFACE on the "
                        + "method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `grouped::count` "
                        + "bound to Supplier<KTable>, Function<"
                        + "Materialized, KTable>, or to a custom SAM "
                        + "whose erased implMethod descriptor matches "
                        + "an unsafe overload). Every Kafka Streams "
                        + "operator compiles into a processor node in "
                        + "the topology graph; every processor node "
                        + "has a name that appears in topology."
                        + "describe(), in the JMX MBean tree under "
                        + "kafka.streams:type=stream-processor-node-"
                        + "metrics,processor-node-id=<name>, and in "
                        + "the internal repartition topic names when "
                        + "the operator is key-changing. When no "
                        + "Named is passed, Streams synthesizes the "
                        + "processor-node name from the topology "
                        + "graph index (e.g. KSTREAM-AGGREGATE-"
                        + "0000000007 and, for key-changing "
                        + "upstreams, app-id-KSTREAM-AGGREGATE-"
                        + "0000000007-repartition). count("
                        + "Materialized) is the SYMMETRIC half-fix "
                        + "to count(Named) — Materialized pins the "
                        + "state-store name and the changelog topic, "
                        + "but does NOT touch the processor-node id, "
                        + "the JMX MBean name, or the auto-generated "
                        + "repartition topic; so count(Materialized."
                        + "as(\"dau-counter-store\")) fixes the "
                        + "state-store half but leaves the processor-"
                        + "node + JMX + repartition naming graph-"
                        + "index-derived. The combination required "
                        + "for full stability is BOTH Named (for the "
                        + "processor-node id, JMX MBean, and "
                        + "repartition topic) AND Materialized (for "
                        + "the state store and changelog) — pair "
                        + "this rule with STREAMS_COUNT_NO_"
                        + "MATERIALIZED. Concrete failure modes: (1) "
                        + "the processor-node JMX MBean disappears "
                        + "on every topology edit — operations "
                        + "dashboards filter on processor-node-id="
                        + "\"KSTREAM-AGGREGATE-0000000007\"; any "
                        + "upstream topology edit shifts the graph "
                        + "index; the next deploy emits MBean "
                        + "KSTREAM-AGGREGATE-0000000008; the "
                        + "dashboard reads zero for the count's "
                        + "process-rate, record-lateness, and "
                        + "dropped-records metrics; the operator "
                        + "believes the counter is broken; (2) the "
                        + "repartition topic for a key-changing "
                        + "upstream rebrands on every topology edit "
                        + "— patterns like stream.selectKey(...)."
                        + "groupByKey().count() force a key change "
                        + "which causes Streams to create an "
                        + "internal repartition topic app-id-"
                        + "KSTREAM-AGGREGATE-0000000007-repartition; "
                        + "after any topology edit the topic name "
                        + "shifts to -0000000008-repartition; the "
                        + "old topic is orphaned on the broker "
                        + "(Streams does not auto-delete it); "
                        + "operations sees an unfamiliar topic in "
                        + "kafka-topics --list and cannot determine "
                        + "which application owns it; (3) count("
                        + "Materialized) is a half-fix that HIDES "
                        + "the problem — a team that named their "
                        + "state store via count(Materialized.as("
                        + "\"dau-counter-store\")) sees the store "
                        + "name pinned in topology.describe() and "
                        + "assumes the operator is fully named; the "
                        + "processor-node name in the same topology"
                        + ".describe() is still KSTREAM-AGGREGATE-"
                        + "0000000007 (graph-index-derived), the JMX "
                        + "MBean is still graph-index-derived, the "
                        + "auto-repartition topic (if key-changed "
                        + "upstream) is still graph-index-derived; "
                        + "the team's monitoring breaks on the next "
                        + "topology edit even though they \"named "
                        + "the operator\"; (4) topology.describe() "
                        + "diff is unreadable across deploys — CI "
                        + "pipelines that diff topology.describe() "
                        + "output against a golden file rebuilt on "
                        + "every PR see spurious renames on every "
                        + "topology edit (KSTREAM-AGGREGATE-"
                        + "0000000005 -> KSTREAM-AGGREGATE-"
                        + "0000000006) even when the count operator "
                        + "itself did not change; the diff is "
                        + "dominated by graph-index renames and the "
                        + "reviewer cannot spot the real semantic "
                        + "changes; (5) INVOKEDYNAMIC method-"
                        + "reference captures bypass naive "
                        + "MethodInsnNode-only lint — Supplier<"
                        + "KTable<K, Long>> counter = grouped::count "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle "
                        + "pointing at KGroupedStream.count()KTable; "
                        + "the user-class bytecode contains zero "
                        + "direct INVOKEINTERFACE on the no-Named "
                        + "overload, only the indy site. Migration: "
                        + "pass an explicit Named pinning the "
                        + "processor-node id — `grouped.count(Named"
                        + ".as(\"dau-counter\"))`. The explicit name "
                        + "pins the processor-node id, the JMX "
                        + "MBean (processor-node-id=dau-counter), "
                        + "and (for key-changing upstream) the "
                        + "repartition topic (app-id-dau-counter-"
                        + "repartition). For full naming coverage, "
                        + "pair Named.as(...) with Materialized.as("
                        + "...) — the 4-arg count(Named, "
                        + "Materialized) overload. The count "
                        + "overloads containing Named are never "
                        + "flagged.");
    }
}
