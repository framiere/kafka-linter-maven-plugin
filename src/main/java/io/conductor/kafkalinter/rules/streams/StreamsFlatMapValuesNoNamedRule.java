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
 * org.apache.kafka.streams.kstream.KStream#flatMapValues(
 * org.apache.kafka.streams.kstream.ValueMapper)
 * KStream.flatMapValues} overload — any descriptor for {@code
 * flatMapValues} on {@link
 * org.apache.kafka.streams.kstream.KStream KStream} that does
 * NOT include a {@link org.apache.kafka.streams.kstream.Named
 * Named} argument and therefore lets the flatMapValues node
 * auto-name from the topology graph index.
 *
 * <p>Predicate is structural — any descriptor for {@code
 * flatMapValues} on {@code KStream} whose argument list
 * contains {@code org/apache/kafka/streams/kstream/Named} is
 * safe; any descriptor that does not is unsafe.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures.
 *
 * <h2>Why no-Named KStream.flatMapValues is a pure observability
 * hazard (no broker-disk component)</h2>
 *
 * <p>{@link
 * org.apache.kafka.streams.kstream.KStream#flatMapValues
 * KStream.flatMapValues} is fan-out but NOT key-changing.
 * Because the partition assignment of every output record is
 * the SAME as the input record's partition, Streams does NOT
 * insert an auto-repartition topic. There is no broker-disk
 * orphan-topic hazard. But the processor-node name is still
 * graph-index-derived ({@code KSTREAM-FLATMAPVALUES-0000000005}),
 * and the metrics/dashboards consequences are AMPLIFIED by the
 * fan-out factor:
 *
 * <ol>
 *   <li>Fan-out-ratio SLO breaks on every topology edit. The
 *       per-node {@code records-out / records-in} ratio is the
 *       primary capacity-planning signal for any fan-out node —
 *       it tells you how much downstream-sink throughput each
 *       input record produces. The metric is keyed by
 *       {@code processor-node-id}; when the topology edits and
 *       the index shifts, your SLO alert on
 *       "flatMapValues fan-out ratio &gt; 100 means a bug" silently
 *       stops firing because the node ID it watches no longer
 *       exists, and the new node ID has no alert attached.</li>
 *   <li>Downstream-sink throughput dashboards rebrand. The
 *       downstream sink (a {@code .to(...)} or downstream
 *       stateful operator) sees N× the input throughput. A
 *       Grafana panel labelled "post-flatMapValues throughput
 *       (KSTREAM-FLATMAPVALUES-0000000005 → sink-topic
 *       produce-rate)" filters either on processor-node ID
 *       (the metric source) or on the downstream sink topic
 *       (whose name is also graph-index-derived for internal
 *       repartition cases, though here the sink is usually a
 *       user-named output). Either way: a topology edit
 *       upstream of the flatMapValues node renames it, the
 *       processor-node-tagged panel goes blank, and the team
 *       has no idea N× the input traffic is still being
 *       produced.</li>
 *   <li>End-to-end latency metric loses continuity. The
 *       {@code record-e2e-latency-{avg,max}} per-processor-node
 *       metric is a primary SLO input for streams apps. Without
 *       a Named name, the metric's identity churns on every
 *       topology edit; long-horizon SLO dashboards (e.g. "p99
 *       e2e latency over 30 days") show a discontinuity each
 *       time the topology is edited, defeating the entire point
 *       of a long-horizon SLO.</li>
 *   <li>Runbook drift. The runbook for "flatMapValues fan-out
 *       too high" references {@code
 *       KSTREAM-FLATMAPVALUES-0000000005} by name. After two
 *       topology edits the node is now
 *       {@code KSTREAM-FLATMAPVALUES-0000000007} and the
 *       runbook leads oncall to inspect a node that no longer
 *       exists. Oncall wastes minutes-to-hours during a real
 *       fan-out-explosion incident chasing the wrong node.</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference captures bypass
 *       naïve {@code MethodInsnNode}-only lint. A
 *       per-input-value-explode factory built as {@code
 *       Function&lt;ValueMapper&lt;V, Iterable&lt;V2&gt;&gt;,
 *       KStream&lt;K, V2&gt;&gt; explode = stream::flatMapValues}
 *       compiles to {@code INVOKEDYNAMIC} whose bsm-args contain
 *       a {@code REF_invokeInterface} Handle pointing at {@code
 *       KStream.flatMapValues(ValueMapper)KStream}.</li>
 * </ol>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named} naming the
 * flatMapValues node — e.g. {@code stream.flatMapValues(
 * v -> v.tags(), Named.as("event-to-tags"))}. The explicit
 * Named name stabilizes the processor-node ID and every metric
 * keyed by it (fan-out ratio, records-out, record-e2e-latency)
 * across topology edits.
 */
public final class StreamsFlatMapValuesNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "flatMapValues";
    private static final String NAMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsFlatMapValuesNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_FLAT_MAP_VALUES_NO_NAMED;
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
                RuleId.STREAMS_FLAT_MAP_VALUES_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.flatMapValues(ValueMapper) — a no-Named "
                        + "overload is reached here — either as a "
                        + "direct INVOKEINTERFACE on the method or as "
                        + "an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `stream::flatMapValues` bound to "
                        + "Function<ValueMapper, KStream> or to a "
                        + "custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). "
                        + "KStream.flatMapValues is fan-out but NOT "
                        + "key-changing — Streams does NOT insert an "
                        + "auto-repartition topic, so there is no "
                        + "broker-disk orphan-topic hazard. But the "
                        + "processor-node name is graph-index-derived "
                        + "(e.g. KSTREAM-FLATMAPVALUES-0000000005), "
                        + "and the metrics/dashboards consequences "
                        + "are AMPLIFIED by the fan-out factor. "
                        + "Concrete failure modes: (1) fan-out-ratio "
                        + "SLO breaks on every topology edit — the "
                        + "per-node records-out / records-in ratio is "
                        + "the primary capacity-planning signal for "
                        + "any fan-out node, telling you how much "
                        + "downstream-sink throughput each input "
                        + "record produces; the metric is keyed by "
                        + "processor-node-id; when the topology edits "
                        + "and the index shifts, your SLO alert on "
                        + "`flatMapValues fan-out ratio > 100 means a "
                        + "bug` silently stops firing because the "
                        + "node ID it watches no longer exists, and "
                        + "the new node ID has no alert attached; (2) "
                        + "downstream-sink throughput dashboards "
                        + "rebrand — the downstream sink sees N* the "
                        + "input throughput; a Grafana panel labelled "
                        + "`post-flatMapValues throughput "
                        + "(KSTREAM-FLATMAPVALUES-0000000005 -> sink-"
                        + "topic produce-rate)` filters on the "
                        + "processor-node ID; a topology edit "
                        + "upstream renames the node, the panel goes "
                        + "blank, and the team has no idea N* the "
                        + "input traffic is still being produced; (3) "
                        + "end-to-end latency metric loses continuity "
                        + "— the record-e2e-latency-{avg,max} per-"
                        + "processor-node metric is a primary SLO "
                        + "input for streams apps; without a Named "
                        + "name, the metric's identity churns on every "
                        + "topology edit; long-horizon SLO dashboards "
                        + "(e.g. `p99 e2e latency over 30 days`) show "
                        + "a discontinuity each time the topology is "
                        + "edited, defeating the entire point of a "
                        + "long-horizon SLO; (4) runbook drift — the "
                        + "runbook for `flatMapValues fan-out too "
                        + "high` references "
                        + "KSTREAM-FLATMAPVALUES-0000000005 by name; "
                        + "after two topology edits the node is now "
                        + "KSTREAM-FLATMAPVALUES-0000000007 and the "
                        + "runbook leads oncall to inspect a node "
                        + "that no longer exists; oncall wastes "
                        + "minutes-to-hours during a real fan-out-"
                        + "explosion incident chasing the wrong node; "
                        + "(5) INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "`Function<ValueMapper<V, Iterable<V2>>, "
                        + "KStream<K, V2>> explode = "
                        + "stream::flatMapValues` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KStream.flatMapValues(ValueMapper)KStream; "
                        + "the user-class bytecode contains zero "
                        + "direct INVOKEINTERFACE on the no-Named "
                        + "overload, only the indy site. Migration: "
                        + "pass an explicit Named — "
                        + "`stream.flatMapValues(v -> v.tags(), "
                        + "Named.as(\"event-to-tags\"))`. The explicit "
                        + "Named name stabilizes the processor-node "
                        + "ID and every metric keyed by it (fan-out "
                        + "ratio, records-out, record-e2e-latency) "
                        + "across topology edits. The flatMapValues "
                        + "overloads containing Named are never "
                        + "flagged.");
    }
}
