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
 * Fires for every reach of a {@link
 * org.apache.kafka.streams.kstream.KTable#toStream()
 * KTable.toStream} overload — any descriptor for {@code
 * toStream} on {@link org.apache.kafka.streams.kstream.KTable
 * KTable} that does NOT include a {@link
 * org.apache.kafka.streams.kstream.Named Named} argument and
 * therefore lets the transition node auto-name from the
 * topology graph index.
 *
 * <p>Predicate is structural — any descriptor for {@code
 * toStream} on {@code KTable} whose argument list contains
 * {@code org/apache/kafka/streams/kstream/Named} is safe; any
 * descriptor that does not is unsafe.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KTable
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code table::toStream} bound to {@link
 * java.util.function.Supplier Supplier&lt;KStream&gt;}, to
 * {@link java.util.function.Function Function&lt;KeyValueMapper,
 * KStream&gt;}, or to a custom SAM whose erased implMethod
 * descriptor matches an unsafe overload).
 *
 * <h2>Why no-Named KTable.toStream is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KTable#toStream
 * KTable.toStream} inserts a KTable→KStream transition node
 * into the topology. The node's name is auto-derived from the
 * graph index — e.g. {@code KTABLE-TOSTREAM-0000000011}. The
 * transition node is the boundary where changelog-tagged
 * updates (each carrying an explicit "previous-value /
 * new-value" diff) are projected to KStream records (one
 * record per update). Every metric panel, runbook step, and
 * trace span at that boundary keys on the node id.
 *
 * <h2>The KeyValueMapper overload doubles the hazard</h2>
 *
 * <p>{@code KTable.toStream(KeyValueMapper)} is the overload
 * that lets you rekey the KStream — e.g. converting an
 * {@code orders-by-id} KTable into an {@code orders-by-
 * customer} KStream. This is <b>key-changing</b>, which means
 * any downstream stateful operator (join, aggregate, groupBy)
 * silently inserts an auto-repartition topic whose name is
 * also graph-index-derived. The downstream auto-repartition is
 * the load-bearing artifact here; on topology edits the
 * repartition topic is renamed and its old version is orphaned
 * on broker disk (Kafka does not auto-delete repartition
 * topics whose producer no longer references them).
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Transition-node throughput dashboards silently
 *       zero.</b> Grafana panels filter on
 *       {@code processor-node-id="KTABLE-TOSTREAM-0000000011"}
 *       for records-out-per-second on the
 *       {@code orders-by-id → orders-stream} transition. One
 *       upstream mapValues insertion shifts the graph index;
 *       the panel reads zero; the operator stops trusting it;
 *       a real backpressure incident (e.g. downstream consumer
 *       lag accumulating because a join is rebuilding) goes
 *       undetected.</li>
 *   <li><b>Auto-repartition topic orphans on every topology
 *       edit (KeyValueMapper overload).</b> {@code
 *       ordersByIdTable.toStream((k, v) -> v.customerId())}
 *       — without an explicit Named — produces a downstream
 *       repartition topic named after the graph-index-derived
 *       node id. Every topology edit renames the repartition
 *       topic; the OLD repartition topic stays on the brokers
 *       (no producer references it; no consumer reads it; the
 *       Streams app does not delete it on shutdown). After
 *       five topology edits the brokers carry 6× the
 *       repartition disk volume needed; broker disk pressure
 *       eventually triggers an out-of-disk incident traceable
 *       to nothing in particular.</li>
 *   <li><b>topology.describe() runbook drift.</b> SRE runbooks
 *       reference the transition node by its auto-generated
 *       name — {@code "if KTABLE-TOSTREAM-0000000011
 *       process-rate drops below 50/s, page the orders team"}.
 *       After any topology edit the named node no longer
 *       exists; the runbook step silently no-ops; the operator
 *       believes they are monitoring the orders-stream
 *       transition but the wrong node (or no node at all) was
 *       targeted.</li>
 *   <li><b>OpenTelemetry transition-boundary spans lose
 *       continuity.</b> The KTable→KStream transition is the
 *       boundary where trace contexts often need to be
 *       enriched (downstream KStream operators may be on
 *       different processing threads); spans labeled with the
 *       transition-node id are partitioned by (pre-edit,
 *       post-edit) node id, breaking parent→child trace
 *       linkage across topology edits.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       transition factory built as {@code Supplier&lt;KStream
 *       &lt;K, V&gt;&gt; transition = table::toStream} compiles
 *       to {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeInterface} Handle pointing at {@code
 *       KTable.toStream()KStream}. The user-class bytecode
 *       contains zero direct {@code INVOKEINTERFACE} on the
 *       no-Named overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named} naming the transition
 * node — e.g. {@code table.toStream(Named.as(
 * "orders-by-id-to-stream"))}. For the KeyValueMapper overload,
 * use {@code table.toStream((k, v) -> v.customerId(),
 * Named.as("orders-by-customer-rekey"))} — the explicit Named
 * also pins the downstream auto-repartition topic name (it is
 * derived from the rekey node name, not the graph index).
 */
public final class StreamsKTableToStreamNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KTABLE);
    private static final String METHOD_NAME = "toStream";
    private static final String NAMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsKTableToStreamNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_KTABLE_TO_STREAM_NO_NAMED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && mi.desc != null
                        && !mi.desc.contains(NAMED_TYPE_TOKEN)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, null);
                    if (h != null && h.getDesc() != null
                            && !h.getDesc().contains(NAMED_TYPE_TOKEN)) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_KTABLE_TO_STREAM_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KTable.toStream — a no-Named overload is reached "
                        + "here — either as a direct INVOKEINTERFACE "
                        + "on the method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `table::toStream` "
                        + "bound to Supplier<KStream>, "
                        + "Function<KeyValueMapper, KStream>, or to a "
                        + "custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). The "
                        + "transition node's name is auto-derived "
                        + "from the topology graph index (e.g. "
                        + "KTABLE-TOSTREAM-0000000011). The "
                        + "KeyValueMapper overload is key-changing — "
                        + "any downstream stateful operator silently "
                        + "inserts an auto-repartition topic whose "
                        + "name is also graph-index-derived. Concrete "
                        + "failure modes: (1) transition-node "
                        + "throughput dashboards silently zero — "
                        + "Grafana panels filter on processor-node-id"
                        + "=\"KTABLE-TOSTREAM-0000000011\" for "
                        + "records-out-per-second on the orders-by-id "
                        + "-> orders-stream transition; one upstream "
                        + "mapValues insertion shifts the graph "
                        + "index; the panel reads zero; the operator "
                        + "stops trusting it; a real backpressure "
                        + "incident (e.g. downstream consumer lag "
                        + "accumulating because a join is rebuilding) "
                        + "goes undetected; (2) auto-repartition "
                        + "topic orphans on every topology edit "
                        + "(KeyValueMapper overload) — "
                        + "ordersByIdTable.toStream((k, v) -> "
                        + "v.customerId()) without an explicit Named "
                        + "produces a downstream repartition topic "
                        + "named after the graph-index-derived node "
                        + "id; every topology edit renames the "
                        + "repartition topic; the OLD repartition "
                        + "topic stays on the brokers (no producer "
                        + "references it; no consumer reads it; the "
                        + "Streams app does not delete it on "
                        + "shutdown); after five topology edits the "
                        + "brokers carry 6x the repartition disk "
                        + "volume needed; broker disk pressure "
                        + "eventually triggers an out-of-disk "
                        + "incident traceable to nothing in "
                        + "particular; (3) topology.describe() "
                        + "runbook drift — SRE runbooks reference the "
                        + "transition node by its auto-generated "
                        + "name (\"if KTABLE-TOSTREAM-0000000011 "
                        + "process-rate drops below 50/s, page the "
                        + "orders team\"); after any topology edit "
                        + "the named node no longer exists; the "
                        + "runbook step silently no-ops; the operator "
                        + "believes they are monitoring the orders-"
                        + "stream transition but the wrong node (or "
                        + "no node at all) was targeted; (4) "
                        + "OpenTelemetry transition-boundary spans "
                        + "lose continuity — the KTable->KStream "
                        + "transition is the boundary where trace "
                        + "contexts often need to be enriched "
                        + "(downstream KStream operators may be on "
                        + "different processing threads); spans "
                        + "labeled with the transition-node id are "
                        + "partitioned by (pre-edit, post-edit) node "
                        + "id, breaking parent->child trace linkage "
                        + "across topology edits; (5) INVOKEDYNAMIC "
                        + "method-reference captures bypass naive "
                        + "MethodInsnNode-only lint — `Supplier<"
                        + "KStream<K, V>> transition = table::toStream` "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle "
                        + "pointing at KTable.toStream()KStream; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit Named naming the transition node "
                        + "— `table.toStream(Named.as(\"orders-by-id-"
                        + "to-stream\"))`. For the KeyValueMapper "
                        + "overload, use `table.toStream((k, v) -> "
                        + "v.customerId(), Named.as(\"orders-by-"
                        + "customer-rekey\"))` — the explicit Named "
                        + "also pins the downstream auto-repartition "
                        + "topic name. The toStream overloads "
                        + "containing Named are never flagged.");
    }
}
