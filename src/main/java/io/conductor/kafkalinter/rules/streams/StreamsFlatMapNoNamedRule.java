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
 * org.apache.kafka.streams.kstream.KStream#flatMap(
 * org.apache.kafka.streams.kstream.KeyValueMapper)
 * KStream.flatMap} overload — any descriptor for {@code flatMap}
 * on {@link org.apache.kafka.streams.kstream.KStream KStream}
 * that does NOT include a {@link
 * org.apache.kafka.streams.kstream.Named Named} argument and
 * therefore lets the flatMap node auto-name from the topology
 * graph index.
 *
 * <p>Predicate is structural — any descriptor for {@code flatMap}
 * on {@code KStream} whose argument list contains {@code
 * org/apache/kafka/streams/kstream/Named} is safe; any descriptor
 * that does not is unsafe.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures.
 *
 * <h2>Why no-Named KStream.flatMap is worse than no-Named map</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#flatMap
 * KStream.flatMap} is KEY-CHANGING AND fan-out (one input → N
 * outputs). Like {@code map}, any downstream stateful operator
 * silently inserts an auto-repartition topic whose name is also
 * graph-index-derived — e.g. {@code app-id-KSTREAM-FLATMAP-
 * 0000000005-repartition}. Unlike {@code map}, the
 * auto-repartition topic carries N× input throughput, where N is
 * the average fan-out factor of the flatMap. This compounds the
 * existing map hazards:
 *
 * <ol>
 *   <li>The orphan-repartition-topic disk-volume hazard is N×
 *       worse on every topology edit — if the flatMap explodes
 *       each input record to ~50 output records (a common shape
 *       for "unpack a batch event into its elements"), the
 *       orphaned repartition topic occupies 50× the broker disk
 *       compared to a no-Named {@code map}.</li>
 *   <li>The full-upstream-replay-on-topology-edit hazard
 *       compounds with the fan-out: replaying T hours of
 *       upstream input does N×T worth of writes to the new
 *       repartition topic, multiplying I/O pressure on brokers
 *       during the replay window.</li>
 *   <li>Because the fan-out factor is data-dependent (the
 *       lambda body decides how many records to emit per input),
 *       it is NOT obvious from the topology DAG how much
 *       repartition-topic throughput is at stake. The author
 *       writing {@code orders.flatMap((k, v) -> v.lineItems()
 *       .stream().map(li -> KeyValue.pair(li.sku(), li))
 *       .toList())} probably does not know that the repartition
 *       topic carries lineItems/order × input-orders/sec.</li>
 * </ol>
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Broker-disk catastrophe traceable to nothing in
 *       particular.</b> Team has {@code orders.flatMap((k, v) ->
 *       v.lineItems().stream().map(li ->
 *       KeyValue.pair(li.sku(), li)).toList())} followed by a
 *       groupByKey+aggregate for inventory-per-sku. The orders
 *       topic does 1k records/sec, average 50 line-items per
 *       order → 50k records/sec on the repartition topic. After
 *       six topology edits over a quarter, the brokers carry
 *       SEVEN repartition topics for the same logical key-change
 *       — each at 50k records/sec × retention.ms. Broker disk
 *       fills; broker page cache thrashes; produce-latency p99
 *       spikes across ALL topics on the affected brokers; the
 *       incident is initially diagnosed as a hot-partition issue
 *       on an unrelated topic.</li>
 *   <li><b>N× upstream-replay-write amplification on every
 *       topology edit.</b> After a topology edit the new
 *       repartition topic is empty. The downstream
 *       groupByKey+aggregate cannot resume from its changelog
 *       because its source is empty; Streams replays the entire
 *       upstream input through the new flatMap node. Replaying
 *       upstream at 1k records/sec produces 50k records/sec into
 *       the new repartition topic — for a 24-hour retention
 *       upstream, the replay writes 50× as many records to
 *       brokers as the upstream contained. Replay window is
 *       hours; broker I/O is saturated for the whole window;
 *       producer-side traffic on unrelated topics suffers.</li>
 *   <li><b>Repartition-throughput dashboards point at a dead
 *       topic.</b> Grafana panels filtering on {@code
 *       topic="app-id-KSTREAM-FLATMAP-0000000005-repartition"}
 *       for produce-rate keep showing the old (now orphan)
 *       topic's metrics — which decay to zero over its
 *       retention window. The dashboard owner believes the
 *       50k-records/sec firehose dropped to zero and pages
 *       oncall; oncall checks consumer lag, sees nothing wrong,
 *       dismisses the alert; meanwhile the actual new
 *       repartition topic (with a different graph-index suffix)
 *       is healthy but unmonitored, and capacity planning is
 *       blind to N× the actual broker load.</li>
 *   <li><b>FlatMap-node {@code process-rate} and {@code
 *       records-out} metrics rebrand on every edit.</b> The
 *       processor-node-id-tagged metrics also churn, breaking
 *       SLO alerts on flatMap-node fan-out ratio (a critical
 *       capacity-planning signal because the fan-out is
 *       data-dependent).</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       rekey-with-fanout factory built as {@code Function&lt;
 *       KeyValueMapper&lt;K, V, Iterable&lt;KeyValue&lt;K2,
 *       V2&gt;&gt;&gt;, KStream&lt;K2, V2&gt;&gt; expand =
 *       stream::flatMap} compiles to {@code INVOKEDYNAMIC}
 *       whose bsm-args contain a {@code REF_invokeInterface}
 *       Handle pointing at {@code
 *       KStream.flatMap(KeyValueMapper)KStream}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Named overload, only the
 *       indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named} naming the flatMap
 * node — e.g. {@code stream.flatMap((k, v) -> v.lineItems()
 * .stream().map(li -> KeyValue.pair(li.sku(), li)).toList(),
 * Named.as("orders-to-line-items"))}. The explicit Named name
 * also flows into the downstream auto-repartition topic name,
 * stabilizing it across topology edits. If the fan-out is the
 * point but the key change is incidental, use {@code
 * flatMapValues} then a separate {@code selectKey} — this
 * isolates the (cheap, no-repartition) fan-out from the
 * (expensive, repartition-inducing) key-change and lets you
 * Named-name each independently.
 */
public final class StreamsFlatMapNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "flatMap";
    private static final String NAMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsFlatMapNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_FLAT_MAP_NO_NAMED;
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
                RuleId.STREAMS_FLAT_MAP_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.flatMap(KeyValueMapper) — a no-Named "
                        + "overload is reached here — either as a "
                        + "direct INVOKEINTERFACE on the method or as "
                        + "an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `stream::flatMap` bound to "
                        + "Function<KeyValueMapper, KStream> or to a "
                        + "custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). "
                        + "KStream.flatMap is KEY-CHANGING AND fan-out "
                        + "— any downstream stateful operator (join, "
                        + "aggregate, groupBy, reduce, count) silently "
                        + "inserts an auto-repartition topic whose "
                        + "name is also graph-index-derived (e.g. "
                        + "app-id-KSTREAM-FLATMAP-0000000005-"
                        + "repartition). This is the load-bearing "
                        + "artifact: it carries N* input throughput "
                        + "permanently, where N is the average fan-out "
                        + "factor of the flatMap (data-dependent — "
                        + "NOT obvious from the topology DAG); it "
                        + "lives on the brokers across deploys; the "
                        + "Streams app does NOT delete it on shutdown; "
                        + "Kafka does NOT auto-delete it when the "
                        + "producer stops referencing it. Concrete "
                        + "failure modes: (1) broker-disk catastrophe "
                        + "traceable to nothing in particular — team "
                        + "has orders.flatMap((k, v) -> v.lineItems()"
                        + ".stream().map(li -> KeyValue.pair(li.sku(),"
                        + " li)).toList()) followed by a "
                        + "groupByKey+aggregate for inventory-per-sku; "
                        + "orders topic does 1k records/sec, average "
                        + "50 line-items per order -> 50k records/sec "
                        + "on the repartition topic; after six "
                        + "topology edits over a quarter, the brokers "
                        + "carry SEVEN repartition topics for the same "
                        + "logical key-change — each at 50k "
                        + "records/sec * retention.ms; broker disk "
                        + "fills; broker page cache thrashes; produce-"
                        + "latency p99 spikes across ALL topics on the "
                        + "affected brokers; the incident is initially "
                        + "diagnosed as a hot-partition issue on an "
                        + "unrelated topic; (2) N* upstream-replay-"
                        + "write amplification on every topology edit "
                        + "— after a topology edit the new repartition "
                        + "topic is empty; the downstream "
                        + "groupByKey+aggregate cannot resume from its "
                        + "changelog because its source is empty; "
                        + "Streams replays the entire upstream input "
                        + "through the new flatMap node; replaying "
                        + "upstream at 1k records/sec produces 50k "
                        + "records/sec into the new repartition topic "
                        + "— for a 24-hour retention upstream, the "
                        + "replay writes 50* as many records to "
                        + "brokers as the upstream contained; replay "
                        + "window is hours; broker I/O is saturated "
                        + "for the whole window; producer-side traffic "
                        + "on unrelated topics suffers; (3) "
                        + "repartition-throughput dashboards point at "
                        + "a dead topic — Grafana panels filtering on "
                        + "topic=\"app-id-KSTREAM-FLATMAP-0000000005-"
                        + "repartition\" for produce-rate keep showing "
                        + "the old (now orphan) topic's metrics which "
                        + "decay to zero over its retention window; "
                        + "the dashboard owner believes the 50k-"
                        + "records/sec firehose dropped to zero and "
                        + "pages oncall; oncall checks consumer lag, "
                        + "sees nothing wrong, dismisses the alert; "
                        + "meanwhile the actual new repartition topic "
                        + "(with a different graph-index suffix) is "
                        + "healthy but unmonitored, and capacity "
                        + "planning is blind to N* the actual broker "
                        + "load; (4) flatMap-node process-rate and "
                        + "records-out metrics rebrand on every edit "
                        + "— the processor-node-id-tagged metrics also "
                        + "churn, breaking SLO alerts on flatMap-node "
                        + "fan-out ratio (a critical capacity-planning "
                        + "signal because the fan-out is data-"
                        + "dependent); (5) INVOKEDYNAMIC method-"
                        + "reference captures bypass naive "
                        + "MethodInsnNode-only lint — `Function<"
                        + "KeyValueMapper<K, V, Iterable<KeyValue<K2, "
                        + "V2>>>, KStream<K2, V2>> expand = "
                        + "stream::flatMap` compiles to INVOKEDYNAMIC "
                        + "whose bsm-args contain a REF_invokeInterface "
                        + "Handle pointing at KStream.flatMap("
                        + "KeyValueMapper)KStream; the user-class "
                        + "bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit Named — `stream.flatMap((k, v) -> "
                        + "v.lineItems().stream().map(li -> KeyValue"
                        + ".pair(li.sku(), li)).toList(), Named.as("
                        + "\"orders-to-line-items\"))`. The explicit "
                        + "Named name also flows into the downstream "
                        + "auto-repartition topic name, stabilizing it "
                        + "across topology edits. If the fan-out is "
                        + "the point but the key change is incidental, "
                        + "use flatMapValues then a separate selectKey "
                        + "— this isolates the (cheap, no-repartition) "
                        + "fan-out from the (expensive, repartition-"
                        + "inducing) key-change and lets you Named-"
                        + "name each independently. The flatMap "
                        + "overloads containing Named are never "
                        + "flagged.");
    }
}
