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
 * org.apache.kafka.streams.kstream.KStream#foreach(
 * org.apache.kafka.streams.kstream.ForeachAction)
 * KStream.foreach} overload — any descriptor for {@code foreach}
 * on {@link org.apache.kafka.streams.kstream.KStream KStream}
 * that does NOT include a {@link
 * org.apache.kafka.streams.kstream.Named Named} argument and
 * therefore lets the foreach terminal node auto-name from the
 * topology graph index.
 *
 * <p>Predicate is structural — any descriptor for {@code foreach}
 * on {@code KStream} whose argument list contains {@code
 * org/apache/kafka/streams/kstream/Named} is safe; any
 * descriptor that does not is unsafe.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface; {@code foreach} returns {@code void}) and
 * {@code INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-Named KStream.foreach has hazards that {@code
 * peek} / {@code mapValues} / {@code filter} do not</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#foreach
 * KStream.foreach} is the TERMINAL operator for side-effect-only
 * sinks: by design it is the place teams call out to external
 * systems (REST endpoints, JDBC writes, S3 puts, OpenTelemetry
 * span emission, audit-log appends, alert dispatch). Because it
 * is terminal and side-effecting, the named identity of the
 * processor node is on the critical path for several downstream
 * properties that no other intermediate operator shares:
 *
 * <ol>
 *   <li>External-side-effect retry-amplification on rebalance
 *       and on restore-from-changelog. Streams guarantees
 *       AT-LEAST-ONCE delivery of records to the foreach node;
 *       on a consumer-group rebalance, the new owner of a
 *       partition replays records from the last committed
 *       offset and re-invokes the foreach action. If the
 *       action is non-idempotent (a POST to a payment-API, an
 *       INSERT without ON CONFLICT, a Slack message) the
 *       side-effect is silently doubled. The dedup story
 *       depends on identifying the foreach node in the DLQ /
 *       audit table: "this row came from the
 *       payment-charge-dispatch foreach". A graph-index name
 *       like {@code KSTREAM-FOREACH-0000000005} provides no
 *       such identification, AND it changes on every topology
 *       edit, so historical audit rows reference a node that
 *       no longer exists. Idempotency keys cannot be derived
 *       from the node identity because the node identity is
 *       not stable.</li>
 *   <li>Terminal-node throughput metrics are the primary
 *       backpressure signal. {@code foreach} runs the action
 *       SYNCHRONOUSLY on the StreamThread — the per-node
 *       {@code process-rate} and {@code process-latency} are
 *       the only metrics that tell you whether the I/O inside
 *       the action is capping topology throughput. These
 *       metrics are tagged by {@code processor-node-id}; when
 *       the topology edits and the index shifts, the SLO
 *       alert "foreach process-latency p99 &gt; 50ms means the
 *       downstream system is degrading" silently stops firing
 *       because the node ID it watches no longer exists, and
 *       the new node ID has no alert attached.</li>
 *   <li>JMX MBean naming and runbook drift. Streams exposes a
 *       per-processor-node MBean under {@code
 *       kafka.streams:type=stream-processor-node-metrics,
 *       thread-id=...,processor-node-id=KSTREAM-FOREACH-
 *       0000000005}. The runbook for "foreach lagging /
 *       hanging on external I/O" references the MBean by
 *       exact name. After two topology edits the MBean is now
 *       {@code KSTREAM-FOREACH-0000000007} and the runbook
 *       leads oncall to inspect a node that no longer exists.
 *       Oncall wastes minutes-to-hours during a real external-
 *       system-outage incident chasing the wrong MBean.</li>
 *   <li>Silent at-least-once double-side-effect during state-
 *       store restore. When a partition migrates and Streams
 *       restores the corresponding state store from the
 *       changelog topic, the foreach action runs again for
 *       every record in the restore window (Streams cannot
 *       distinguish "restoring state" from "new live input"
 *       at the foreach node — both look like records flowing
 *       through). If the team's runbook says "on rebalance,
 *       check the DLQ table for double-side-effect entries
 *       tagged with the foreach node name", that runbook is
 *       broken when the node name churns on every topology
 *       edit.</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference captures bypass
 *       naïve {@code MethodInsnNode}-only lint. A sink-dispatch
 *       factory built as {@code Consumer&lt;
 *       ForeachAction&lt;K, V&gt;&gt; dispatch = stream::foreach}
 *       compiles to {@code INVOKEDYNAMIC} whose bsm-args
 *       contain a {@code REF_invokeInterface} Handle pointing
 *       at {@code KStream.foreach(ForeachAction)V}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Named overload, only the
 *       indy site.</li>
 * </ol>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named} naming the foreach
 * node — e.g. {@code stream.foreach((k, v) -> paymentApi.charge(
 * k, v), Named.as("payment-charge-dispatch"))}. The explicit
 * Named name stabilizes the processor-node ID, the JMX MBean
 * name, every metric keyed by node-id (process-rate, process-
 * latency, record-e2e-latency), and the audit/DLQ identifier
 * for any non-idempotent side-effect. Additionally, audit
 * whether the side-effect belongs inside the topology at all:
 * a {@code .to(sink-topic)} followed by a separate consumer
 * group that drives the external I/O decouples Streams
 * throughput from external-system latency and gives you
 * independent retry / DLQ / scaling controls.
 */
public final class StreamsForEachNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "foreach";
    private static final String NAMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsForEachNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_FOREACH_NO_NAMED;
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
                RuleId.STREAMS_FOREACH_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.foreach(ForeachAction) — a no-Named "
                        + "overload is reached here — either as a "
                        + "direct INVOKEINTERFACE on the method or as "
                        + "an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `stream::foreach` bound to "
                        + "Consumer<ForeachAction> or to a custom SAM "
                        + "whose erased implMethod descriptor matches "
                        + "an unsafe overload). KStream.foreach is the "
                        + "TERMINAL operator for side-effect-only sinks "
                        + "— REST endpoints, JDBC writes, S3 puts, "
                        + "OpenTelemetry spans, audit-log appends, "
                        + "alert dispatch — and the named identity of "
                        + "the processor node is on the critical path "
                        + "for several downstream properties that no "
                        + "other intermediate operator shares. "
                        + "Concrete failure modes: (1) external-side-"
                        + "effect retry-amplification on rebalance and "
                        + "on restore-from-changelog — Streams "
                        + "guarantees AT-LEAST-ONCE delivery; on a "
                        + "consumer-group rebalance, the new owner of "
                        + "a partition replays records from the last "
                        + "committed offset and re-invokes the foreach "
                        + "action; if the action is non-idempotent (a "
                        + "POST to a payment-API, an INSERT without "
                        + "ON CONFLICT, a Slack message) the side-"
                        + "effect is silently doubled; the dedup story "
                        + "depends on identifying the foreach node in "
                        + "the DLQ / audit table (`this row came from "
                        + "the payment-charge-dispatch foreach`); a "
                        + "graph-index name like KSTREAM-FOREACH-"
                        + "0000000005 provides no such identification, "
                        + "AND it changes on every topology edit, so "
                        + "historical audit rows reference a node that "
                        + "no longer exists; idempotency keys cannot "
                        + "be derived from the node identity because "
                        + "the node identity is not stable; (2) "
                        + "terminal-node throughput metrics are the "
                        + "primary backpressure signal — foreach runs "
                        + "the action SYNCHRONOUSLY on the "
                        + "StreamThread, the per-node process-rate "
                        + "and process-latency are the only metrics "
                        + "that tell you whether the I/O inside the "
                        + "action is capping topology throughput; "
                        + "these metrics are tagged by processor-"
                        + "node-id; when the topology edits and the "
                        + "index shifts, the SLO alert `foreach "
                        + "process-latency p99 > 50ms means the "
                        + "downstream system is degrading` silently "
                        + "stops firing because the node ID it "
                        + "watches no longer exists, and the new node "
                        + "ID has no alert attached; (3) JMX MBean "
                        + "naming and runbook drift — Streams exposes "
                        + "a per-processor-node MBean under "
                        + "kafka.streams:type=stream-processor-node-"
                        + "metrics,thread-id=...,processor-node-id="
                        + "KSTREAM-FOREACH-0000000005; the runbook "
                        + "for `foreach lagging / hanging on external "
                        + "I/O` references the MBean by exact name; "
                        + "after two topology edits the MBean is now "
                        + "KSTREAM-FOREACH-0000000007 and the runbook "
                        + "leads oncall to inspect a node that no "
                        + "longer exists; oncall wastes minutes-to-"
                        + "hours during a real external-system-outage "
                        + "incident chasing the wrong MBean; (4) "
                        + "silent at-least-once double-side-effect "
                        + "during state-store restore — when a "
                        + "partition migrates and Streams restores "
                        + "the corresponding state store from the "
                        + "changelog topic, the foreach action runs "
                        + "again for every record in the restore "
                        + "window (Streams cannot distinguish "
                        + "`restoring state` from `new live input` at "
                        + "the foreach node — both look like records "
                        + "flowing through); if the team's runbook "
                        + "says `on rebalance, check the DLQ table "
                        + "for double-side-effect entries tagged with "
                        + "the foreach node name`, that runbook is "
                        + "broken when the node name churns on every "
                        + "topology edit; (5) INVOKEDYNAMIC method-"
                        + "reference captures bypass naive "
                        + "MethodInsnNode-only lint — `Consumer<"
                        + "ForeachAction<K, V>> dispatch = "
                        + "stream::foreach` compiles to INVOKEDYNAMIC "
                        + "whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KStream.foreach(ForeachAction)V; the user-"
                        + "class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit Named — `stream.foreach((k, v) -> "
                        + "paymentApi.charge(k, v), Named.as("
                        + "\"payment-charge-dispatch\"))`. The "
                        + "explicit Named name stabilizes the "
                        + "processor-node ID, the JMX MBean name, "
                        + "every metric keyed by node-id (process-"
                        + "rate, process-latency, record-e2e-latency), "
                        + "and the audit/DLQ identifier for any non-"
                        + "idempotent side-effect. Additionally, "
                        + "audit whether the side-effect belongs "
                        + "inside the topology at all: a "
                        + ".to(sink-topic) followed by a separate "
                        + "consumer group that drives the external I/O "
                        + "decouples Streams throughput from external-"
                        + "system latency and gives you independent "
                        + "retry / DLQ / scaling controls. The foreach "
                        + "overloads containing Named are never "
                        + "flagged.");
    }
}
