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
 * Fires for every reach of the zero-argument overload of
 * {@link org.apache.kafka.streams.kstream.KStream#repartition()
 * KStream.repartition} — i.e. the EXPLICIT repartition operator
 * invoked with no {@link
 * org.apache.kafka.streams.kstream.Repartitioned Repartitioned}
 * argument.
 *
 * <p>The unsafe descriptor is exactly {@code
 * ()Lorg/apache/kafka/streams/kstream/KStream;} on {@link
 * org.apache.kafka.streams.kstream.KStream KStream}. The safe
 * sibling {@code repartition(Repartitioned)} (descriptor {@code
 * (Lorg/apache/kafka/streams/kstream/Repartitioned;)Lorg/apache/
 * kafka/streams/kstream/KStream;}) is never flagged.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code stream::repartition} bound to {@link
 * java.util.function.Supplier Supplier&lt;KStream&gt;} or to a
 * custom 0-arg SAM whose erased implMethod descriptor matches
 * exactly {@code ()KStream}).
 *
 * <h2>Why no-Repartitioned {@code repartition()} is a hazard
 * even though the call is EXPLICIT</h2>
 *
 * <p>Unlike auto-repartition (which the DSL inserts silently
 * when a downstream operator demands co-partitioning after a
 * key-changing op), {@code KStream.repartition()} is an explicit
 * operator the developer wrote on purpose. That is the trap:
 * because the call is visibly there in source, reviewers assume
 * the topic name is also under the developer's control. It is
 * not. The 0-arg overload defers naming to Streams' internal
 * NodeIdAllocator, which assigns the topic a graph-index-derived
 * name such as {@code app-id-KSTREAM-REPARTITION-0000000005-
 * repartition}. Every other artifact bound to this node (the
 * processor-node MBean, the topology.describe() entry, the
 * Open-Telemetry span, every Grafana panel) keys on the same
 * shifting graph index.
 *
 * <p>An explicit {@code repartition()} is usually inserted for
 * one of three reasons: (a) to PIN partitioning before a
 * downstream stateful operator chain (most common — e.g.
 * before a sequence of joins that need consistent co-
 * partitioning), (b) to force a partition-count change via a
 * {@link
 * org.apache.kafka.streams.kstream.Repartitioned#withNumberOfPartitions
 * Repartitioned.withNumberOfPartitions}, or (c) to fence a
 * downstream sub-topology onto its own task pool. Reasons (b)
 * and (c) require {@code Repartitioned} anyway, so the 0-arg
 * overload almost always corresponds to reason (a) — and reason
 * (a) is the case where the graph-index-derived topic name does
 * the most damage: any topology edit upstream shifts the index,
 * the new {@code repartition()} produces into a NEW topic, the
 * OLD topic is orphaned with full retention, and the downstream
 * (stateful) operator chain cold-starts from offset 0 of the
 * new empty topic.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Downstream join cold-starts on every topology
 *       edit.</b> Team has {@code orders.selectKey((k, v) ->
 *       v.customerId()).repartition().join(payments, ...)} —
 *       the explicit {@code repartition()} is there to pin the
 *       co-partitioning. After one upstream topology edit
 *       (someone inserts a {@code peek(...)} for debugging
 *       before {@code selectKey}), the repartition node's graph
 *       index shifts from {@code KSTREAM-REPARTITION-0000000005}
 *       to {@code KSTREAM-REPARTITION-0000000006}. The new topic
 *       is empty; the downstream join produces NULL for every
 *       record falling inside the join window; the old topic
 *       sits on broker disk with full retention.</li>
 *   <li><b>Repartition-topic orphan accumulation.</b> Each
 *       topology edit upstream of the {@code repartition()}
 *       leaves the previous repartition topic orphaned (Kafka
 *       does not auto-delete repartition topics whose producer
 *       no longer references them; the topic name is now off-
 *       graph). For a team with 10 explicit repartitions across
 *       their topologies and an average of 8 topology edits per
 *       repartition per year, that's 80 orphan repartition
 *       topics per year — each carrying the full upstream
 *       throughput's retention-bound disk footprint until
 *       someone runs a manual cleanup script.</li>
 *   <li><b>topology.describe() runbook drift.</b> SRE runbooks
 *       reference the repartition node by its auto-generated
 *       name — {@code "if KSTREAM-REPARTITION-0000000005 process
 *       -rate drops below 50/s, page the orders team"}. After
 *       any topology edit the named node no longer exists; the
 *       runbook step silently no-ops; the operator believes
 *       they are monitoring the orders-customer-rekey
 *       repartition but the wrong node (or no node at all) was
 *       targeted.</li>
 *   <li><b>Grafana repartition-throughput panels rebrand.</b>
 *       Panels filter on {@code topic="app-id-KSTREAM-
 *       REPARTITION-0000000005-repartition"} or {@code
 *       processor-node-id="KSTREAM-REPARTITION-0000000005"} for
 *       repartition-write-rate, repartition-end-to-end-latency-
 *       p99, and repartition-bytes-in-per-sec. ALL rebrand on
 *       any topology edit upstream; capacity planning for the
 *       repartition step becomes a guessing game; oncall
 *       eventually deletes the panels for being noisy.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       repartition factory built as {@code Supplier&lt;KStream
 *       &lt;K, V&gt;&gt; repartitionFactory = stream::repartition}
 *       compiles to an {@code INVOKEDYNAMIC} site whose
 *       bsm-args contain a {@code REF_invokeInterface} Handle
 *       pointing at {@code KStream.repartition()KStream}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Repartitioned overload, only
 *       the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Repartitioned Repartitioned}
 * with an explicit name — e.g. {@code
 * stream.repartition(Repartitioned.as("orders-by-customer-
 * rekey"))}. The explicit name pins the repartition topic name
 * ({@code app-id-orders-by-customer-rekey-repartition}) and the
 * processor-node id ({@code orders-by-customer-rekey-
 * repartition-filter} / {@code -sink} / {@code -source}),
 * stabilizing them across topology edits. If a partition-count
 * change is also needed, chain {@link
 * org.apache.kafka.streams.kstream.Repartitioned#withNumberOfPartitions
 * .withNumberOfPartitions(...)}.
 */
public final class StreamsRepartitionNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "repartition";
    private static final String UNSAFE_DESC =
            "()Lorg/apache/kafka/streams/kstream/KStream;";

    private final Severity severity;

    public StreamsRepartitionNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_REPARTITION_NO_NAMED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && UNSAFE_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, UNSAFE_DESC);
                    if (h != null) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_REPARTITION_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.repartition() — the no-Repartitioned "
                        + "0-arg overload is reached here — either as "
                        + "a direct INVOKEINTERFACE on the method or "
                        + "as an INVOKEDYNAMIC method-reference "
                        + "capture (e.g. `stream::repartition` bound "
                        + "to Supplier<KStream> or to a custom 0-arg "
                        + "SAM whose erased implMethod descriptor "
                        + "matches ()KStream exactly). Unlike auto-"
                        + "repartition (which the DSL inserts silently "
                        + "when a downstream operator demands co-"
                        + "partitioning after a key-changing op), "
                        + "KStream.repartition() is an EXPLICIT "
                        + "operator the developer wrote on purpose — "
                        + "and that is the trap: because the call is "
                        + "visibly there in source, reviewers assume "
                        + "the topic name is also under the "
                        + "developer's control; it is not. The 0-arg "
                        + "overload defers naming to Streams' internal "
                        + "NodeIdAllocator, which assigns the topic a "
                        + "graph-index-derived name such as `app-id-"
                        + "KSTREAM-REPARTITION-0000000005-repartition`. "
                        + "Every other artifact bound to this node "
                        + "(processor-node MBean, topology.describe() "
                        + "entry, OpenTelemetry span, every Grafana "
                        + "panel) keys on the same shifting graph "
                        + "index. Concrete failure modes: (1) "
                        + "downstream join cold-starts on every "
                        + "topology edit — team has orders.selectKey"
                        + "((k, v) -> v.customerId()).repartition()"
                        + ".join(payments, ...), the explicit "
                        + "repartition() is there to pin the co-"
                        + "partitioning; after one upstream topology "
                        + "edit (someone inserts a peek(...) for "
                        + "debugging before selectKey) the repartition "
                        + "node's graph index shifts from KSTREAM-"
                        + "REPARTITION-0000000005 to KSTREAM-"
                        + "REPARTITION-0000000006, the new topic is "
                        + "empty, the downstream join produces NULL "
                        + "for every record falling inside the join "
                        + "window, and the old topic sits on broker "
                        + "disk with full retention; (2) repartition-"
                        + "topic orphan accumulation — each topology "
                        + "edit upstream of the repartition() leaves "
                        + "the previous repartition topic orphaned "
                        + "(Kafka does not auto-delete repartition "
                        + "topics whose producer no longer references "
                        + "them; the topic name is now off-graph); "
                        + "for a team with 10 explicit repartitions "
                        + "across their topologies and an average of "
                        + "8 topology edits per repartition per year, "
                        + "that's 80 orphan repartition topics per "
                        + "year, each carrying the full upstream "
                        + "throughput's retention-bound disk "
                        + "footprint until someone runs a manual "
                        + "cleanup script; (3) topology.describe() "
                        + "runbook drift — SRE runbooks reference the "
                        + "repartition node by its auto-generated "
                        + "name (\"if KSTREAM-REPARTITION-0000000005 "
                        + "process-rate drops below 50/s, page the "
                        + "orders team\"); after any topology edit "
                        + "the named node no longer exists, the "
                        + "runbook step silently no-ops, the operator "
                        + "believes they are monitoring the orders-"
                        + "customer-rekey repartition but the wrong "
                        + "node (or no node at all) was targeted; "
                        + "(4) Grafana repartition-throughput panels "
                        + "rebrand — panels filter on topic=\"app-id-"
                        + "KSTREAM-REPARTITION-0000000005-repartition"
                        + "\" or processor-node-id=\"KSTREAM-"
                        + "REPARTITION-0000000005\" for repartition-"
                        + "write-rate, repartition-end-to-end-latency-"
                        + "p99, and repartition-bytes-in-per-sec; ALL "
                        + "rebrand on any topology edit upstream; "
                        + "capacity planning for the repartition step "
                        + "becomes a guessing game; oncall eventually "
                        + "deletes the panels for being noisy; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "`Supplier<KStream<K, V>> repartitionFactory "
                        + "= stream::repartition` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KStream.repartition()KStream; the user-"
                        + "class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Repartitioned "
                        + "overload, only the indy site. Migration: "
                        + "pass an explicit Repartitioned with an "
                        + "explicit name — `stream.repartition("
                        + "Repartitioned.as(\"orders-by-customer-"
                        + "rekey\"))`. The explicit name pins the "
                        + "repartition topic name (`app-id-orders-by-"
                        + "customer-rekey-repartition`) and the "
                        + "processor-node id (`orders-by-customer-"
                        + "rekey-repartition-filter` / `-sink` / `"
                        + "-source`), stabilizing them across "
                        + "topology edits. If a partition-count "
                        + "change is also needed, chain "
                        + ".withNumberOfPartitions(...). The "
                        + "repartition(Repartitioned) overload is "
                        + "never flagged.");
    }
}
