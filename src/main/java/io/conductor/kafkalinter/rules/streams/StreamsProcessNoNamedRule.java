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
 * org.apache.kafka.streams.kstream.KStream#process(
 * org.apache.kafka.streams.processor.api.ProcessorSupplier,
 * String...) KStream.process(ProcessorSupplier, String...)} or
 * {@link org.apache.kafka.streams.kstream.KStream#processValues(
 * org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier,
 * String...) KStream.processValues(FixedKeyProcessorSupplier,
 * String...)} overload that does NOT carry a {@link
 * org.apache.kafka.streams.kstream.Named} argument.
 *
 * <p>Predicate is structural — any descriptor for these two
 * method names whose argument list contains {@code Named} is
 * safe; any descriptor that does not is unsafe. This catches all
 * current overload pairs:
 *
 * <ul>
 *   <li>{@code process(ProcessorSupplier, String...)} — UNSAFE</li>
 *   <li>{@code process(ProcessorSupplier, Named, String...)} — SAFE</li>
 *   <li>{@code processValues(FixedKeyProcessorSupplier, String...)} — UNSAFE</li>
 *   <li>{@code processValues(FixedKeyProcessorSupplier, Named, String...)} — SAFE</li>
 * </ul>
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code stream::process} bound to a SAM whose
 * erased implMethod descriptor matches one of the unsafe
 * overloads).
 *
 * <h2>Why no-Named process / processValues is a correctness
 * hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#process
 * KStream.process} attaches a user-supplied {@code
 * ProcessorSupplier} to the topology graph as a new processor
 * node. Without an explicit {@link
 * org.apache.kafka.streams.kstream.Named#as(String)}, Streams
 * synthesises a node name of the form {@code KSTREAM-PROCESSOR-
 * 0000000005} from the topology builder's monotonically-incrementing
 * graph index. Every metric, every state-store binding, every
 * {@code topology.describe()} block, every distributed-trace span
 * derives its label from that node name.
 *
 * <h2>Concrete failure modes (carried verbatim into the violation
 * message)</h2>
 *
 * <ul>
 *   <li><b>Per-node metric panels silently empty after a topology
 *       edit.</b> A team has a Grafana dashboard with panels
 *       filtered on {@code node-id="KSTREAM-PROCESSOR-0000000005"}
 *       — process-rate, dropped-records-rate, processing-latency
 *       per node, all tagged by the synthesised node name. Team
 *       ships a hotfix that adds one upstream filter; graph index
 *       shifts; the processor now reports under {@code KSTREAM-
 *       PROCESSOR-0000000007}; every Grafana panel filtered on the
 *       old node name reads zero indefinitely; oncall stops looking
 *       at those panels because they look broken; a real
 *       process-rate collapse two weeks later is invisible until a
 *       customer complaint.</li>
 *   <li><b>topology.describe() runbook drift.</b> SRE runbooks
 *       reference processor nodes by their auto-generated names —
 *       {@code "if KSTREAM-PROCESSOR-0000000005 backs up, restart
 *       the app with --threads=8"}. After any topology edit, the
 *       referenced node no longer exists by that name; the runbook
 *       step silently no-ops; the operator believes they have
 *       restarted with the new thread count but the wrong node
 *       was targeted (or no node at all).</li>
 *   <li><b>State-store-to-processor mapping drift.</b> A processor
 *       is attached to two state stores via the {@code String...
 *       stateStoreNames} varargs. The {@code topology.describe()}
 *       output shows {@code Processor: KSTREAM-PROCESSOR-
 *       0000000005 (stores: [orders-store, payments-store])}.
 *       After a topology edit, the same logical processor now
 *       appears as {@code KSTREAM-PROCESSOR-0000000007}; debugging
 *       a missing state-store binding requires re-deriving which
 *       new auto-name corresponds to which logical processor —
 *       a manual correlation step that grows linear in the number
 *       of {@code process()} call sites.</li>
 *   <li><b>OpenTelemetry / distributed-trace spans lose continuity.</b>
 *       Kafka Streams + OTel instrumentation labels each processor
 *       span with the node name. A trace from upstream Kafka
 *       producer → Streams app → downstream Kafka producer carries
 *       spans tagged {@code kafka.streams.node.id=KSTREAM-
 *       PROCESSOR-0000000005}. After a topology edit, the same
 *       business operation now emits spans tagged with the new
 *       auto-name; trace-aggregation queries that filter by old
 *       node name return zero matches; APM dashboards silently
 *       break.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures bypass
 *       naive {@code MethodInsnNode}-only lint.</b> A processor
 *       factory built as {@code Supplier<KStream<K, V>> attach =
 *       () -> stream.process(supplier, "orders-store")} compiles
 *       to {@code INVOKEDYNAMIC} via a synthetic lambda body that
 *       contains the {@code INVOKEINTERFACE}; a direct
 *       method-reference capture like {@code Function<
 *       ProcessorSupplier<K, V, K, V>, KStream<K, V>> attach =
 *       stream::process} compiles to {@code INVOKEDYNAMIC} whose
 *       bsm-args contain a {@code REF_invokeInterface} Handle
 *       pointing at the unsafe overload exactly.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named#as(String)} — e.g.
 * {@code stream.process(supplier, Named.as("enrich-orders"),
 * "orders-store")}. The chosen name is stable across topology
 * edits because the user wrote it down.
 */
public final class StreamsProcessNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final Set<String> METHOD_NAMES = Set.of("process", "processValues");
    private static final String NAMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsProcessNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROCESS_NO_NAMED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAMES.contains(mi.name)
                        && mi.desc != null
                        && !mi.desc.contains(NAMED_TYPE_TOKEN)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && findUnsafeProcessHandle(indy) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private static Handle findUnsafeProcessHandle(InvokeDynamicInsnNode indy) {
        for (String name : METHOD_NAMES) {
            Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, name, null);
            if (h != null && h.getDesc() != null && !h.getDesc().contains(NAMED_TYPE_TOKEN)) {
                return h;
            }
        }
        return null;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_PROCESS_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.process(ProcessorSupplier, String...) / "
                        + "processValues(FixedKeyProcessorSupplier, "
                        + "String...) — a no-Named overload is reached "
                        + "here — either as a direct INVOKEINTERFACE on "
                        + "the method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `stream::process` "
                        + "bound to a SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). "
                        + "KStream.process attaches a user-supplied "
                        + "ProcessorSupplier to the topology graph as a "
                        + "new processor node; without an explicit "
                        + "Named.as(\"...\") Streams synthesises a node "
                        + "name like `KSTREAM-PROCESSOR-0000000005` "
                        + "from the topology builder's monotonically-"
                        + "incrementing graph index. Concrete failure "
                        + "modes: (1) per-node metric panels silently "
                        + "empty after a topology edit — Grafana panels "
                        + "filter on `node-id=\"KSTREAM-PROCESSOR-"
                        + "0000000005\"` for process-rate, dropped-"
                        + "records-rate, processing-latency per node; "
                        + "team adds one upstream filter; graph index "
                        + "shifts; the processor now reports under "
                        + "`KSTREAM-PROCESSOR-0000000007`; every panel "
                        + "filtered on the old node name reads zero "
                        + "indefinitely; oncall stops looking at those "
                        + "panels because they look broken; a real "
                        + "process-rate collapse two weeks later is "
                        + "invisible until a customer complaint; (2) "
                        + "topology.describe() runbook drift — SRE "
                        + "runbooks reference nodes by auto-generated "
                        + "names (`if KSTREAM-PROCESSOR-0000000005 backs "
                        + "up, restart with --threads=8`); after any "
                        + "topology edit the referenced node no longer "
                        + "exists by that name; runbook step silently "
                        + "no-ops; operator believes they have restarted "
                        + "with the new thread count but the wrong node "
                        + "was targeted; (3) state-store-to-processor "
                        + "mapping drift — a processor attached to two "
                        + "state stores via the `String... "
                        + "stateStoreNames` varargs shows in describe() "
                        + "as `Processor: KSTREAM-PROCESSOR-0000000005 "
                        + "(stores: [orders-store, payments-store])`; "
                        + "after a topology edit the same logical "
                        + "processor appears as `KSTREAM-PROCESSOR-"
                        + "0000000007`; debugging a missing state-store "
                        + "binding requires re-deriving which new auto-"
                        + "name corresponds to which logical processor "
                        + "— a manual correlation step that grows "
                        + "linear in the number of process() call sites; "
                        + "(4) OpenTelemetry trace spans lose "
                        + "continuity — Kafka Streams + OTel labels each "
                        + "processor span with the node name; a trace "
                        + "from upstream producer → Streams app → "
                        + "downstream producer carries spans tagged "
                        + "`kafka.streams.node.id=KSTREAM-PROCESSOR-"
                        + "0000000005`; after a topology edit the same "
                        + "business operation emits spans with the new "
                        + "auto-name; trace-aggregation queries that "
                        + "filter by old node name return zero matches; "
                        + "APM dashboards silently break; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "`Function<ProcessorSupplier<K, V, K, V>, "
                        + "KStream<K, V>> attach = stream::process` "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle pointing "
                        + "at the unsafe overload; the user-class "
                        + "bytecode contains zero direct INVOKEINTERFACE "
                        + "on the no-Named overload, only the indy site. "
                        + "Migration: pass an explicit Named.as(\"...\") "
                        + "— `stream.process(supplier, Named.as(\""
                        + "enrich-orders\"), \"orders-store\")`. The "
                        + "chosen name is stable across topology edits "
                        + "because the user wrote it down. The Named "
                        + "overloads are never flagged by this rule.");
    }
}
