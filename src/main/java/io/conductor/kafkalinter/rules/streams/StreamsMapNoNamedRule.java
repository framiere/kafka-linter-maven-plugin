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
 * org.apache.kafka.streams.kstream.KStream#map(
 * org.apache.kafka.streams.kstream.KeyValueMapper)
 * KStream.map} overload — any descriptor for {@code map} on
 * {@link org.apache.kafka.streams.kstream.KStream KStream}
 * that does NOT include a {@link
 * org.apache.kafka.streams.kstream.Named Named} argument and
 * therefore lets the map node auto-name from the topology
 * graph index.
 *
 * <p>Predicate is structural — any descriptor for {@code map}
 * on {@code KStream} whose argument list contains {@code
 * org/apache/kafka/streams/kstream/Named} is safe; any
 * descriptor that does not is unsafe.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls
 * (KStream is an interface) and {@code INVOKEDYNAMIC}
 * method-reference captures.
 *
 * <h2>Why no-Named KStream.map is THE most dangerous of the
 * unnamed-node hazards</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#map
 * KStream.map} is KEY-CHANGING. Any downstream stateful
 * operator (join, aggregate, groupBy, reduce, count) silently
 * inserts an auto-repartition topic whose name is also
 * graph-index-derived — e.g. {@code app-id-KSTREAM-MAP-
 * 0000000005-repartition}. This is the load-bearing artifact
 * because:
 *
 * <ol>
 *   <li>It carries 1× input throughput permanently (every
 *       input record produces one repartition-topic record).
 *       </li>
 *   <li>It lives on the brokers across deploys — it's a real
 *       Kafka topic with broker-disk volume, broker-RAM page
 *       cache, replication factor, and partition leadership
 *       cost.</li>
 *   <li>The Streams app does NOT delete it on shutdown.
 *       Kafka does NOT auto-delete it when the producer
 *       stops referencing it.</li>
 *   <li>Every topology edit upstream of the {@code map}
 *       renames the auto-repartition topic; the old one is
 *       orphaned on broker disk; the new one starts from
 *       zero, forcing a full upstream replay to repopulate
 *       it.</li>
 * </ol>
 *
 * <p>This compounds: every topology edit doubles the
 * repartition-topic surface area on disk, and forces a full
 * upstream-input replay to repopulate the new repartition
 * topic before the downstream stateful operator can resume.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Broker-disk pressure incident traceable to
 *       nothing in particular.</b> Team has {@code
 *       orders.map((k, v) -> KeyValue.pair(v.customerId(),
 *       v))} followed by a groupByKey+count for
 *       orders-per-customer. After six topology edits over a
 *       quarter, the brokers carry SEVEN repartition topics
 *       for the same logical key-change, each with full
 *       retention.ms worth of records. Broker disk usage
 *       climbs unexplained; the SRE team eventually does a
 *       manual {@code kafka-topics --describe} sweep and
 *       discovers the orphans, but cannot safely delete them
 *       without a maintenance window because verifying they
 *       are truly unreferenced requires inspecting every
 *       deployed Streams app's topology.</li>
 *   <li><b>Full upstream replay on every topology edit.</b>
 *       After a topology edit the new repartition topic is
 *       empty. The downstream groupByKey+count cannot resume
 *       from its changelog because its source (the
 *       repartition topic) is empty; Streams replays the
 *       entire upstream input through the new map node to
 *       repopulate the repartition topic. For a high-volume
 *       topic this is hours of replay during which the
 *       downstream count is stale. Worse, if the upstream
 *       input has retention.ms shorter than the replay
 *       duration, you've lost data permanently.</li>
 *   <li><b>Repartition-throughput dashboards point at a
 *       dead topic.</b> Grafana panels filtering on {@code
 *       topic="app-id-KSTREAM-MAP-0000000005-repartition"}
 *       for produce-rate keep showing the old (now orphan)
 *       topic's metrics — which decay to zero over its
 *       retention window. The dashboard owner believes
 *       throughput dropped to zero and pages oncall; oncall
 *       checks consumer lag, sees nothing wrong, dismisses
 *       the alert; meanwhile the actual new repartition
 *       topic (with a different graph-index suffix) is
 *       healthy but unmonitored.</li>
 *   <li><b>Map-node {@code process-rate} and {@code
 *       record-e2e-latency} metrics rebrand on every edit.
 *       </b> The processor-node-id-tagged metrics also churn,
 *       breaking SLO alerts on map-node end-to-end latency.
 *       </li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       rekey factory built as {@code Function&lt;
 *       KeyValueMapper&lt;K, V, KeyValue&lt;K2, V2&gt;&gt;,
 *       KStream&lt;K2, V2&gt;&gt; rekey = stream::map}
 *       compiles to {@code INVOKEDYNAMIC} whose bsm-args
 *       contain a {@code REF_invokeInterface} Handle pointing
 *       at {@code KStream.map(KeyValueMapper)KStream}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Named overload, only the
 *       indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named} naming the map node
 * — e.g. {@code stream.map((k, v) -> KeyValue.pair(
 * v.customerId(), v), Named.as("orders-by-customer-rekey"))}.
 * The explicit Named name also flows into the downstream
 * auto-repartition topic name, stabilizing it across topology
 * edits. If no key change is needed, use {@code mapValues}
 * instead — it does NOT insert an auto-repartition topic.
 */
public final class StreamsMapNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "map";
    private static final String NAMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsMapNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_MAP_NO_NAMED;
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
                RuleId.STREAMS_MAP_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.map(KeyValueMapper) — a no-Named overload "
                        + "is reached here — either as a direct "
                        + "INVOKEINTERFACE on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `stream::map` bound to "
                        + "Function<KeyValueMapper, KStream> or to a "
                        + "custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). "
                        + "KStream.map is KEY-CHANGING — any "
                        + "downstream stateful operator (join, "
                        + "aggregate, groupBy, reduce, count) "
                        + "silently inserts an auto-repartition topic "
                        + "whose name is also graph-index-derived "
                        + "(e.g. app-id-KSTREAM-MAP-0000000005-"
                        + "repartition). This is the load-bearing "
                        + "artifact: it carries 1x input throughput "
                        + "permanently; it lives on the brokers "
                        + "across deploys; the Streams app does NOT "
                        + "delete it on shutdown; Kafka does NOT "
                        + "auto-delete it when the producer stops "
                        + "referencing it. Concrete failure modes: "
                        + "(1) broker-disk pressure incident "
                        + "traceable to nothing in particular — team "
                        + "has orders.map((k, v) -> KeyValue.pair("
                        + "v.customerId(), v)) followed by a "
                        + "groupByKey+count for orders-per-customer; "
                        + "after six topology edits over a quarter, "
                        + "the brokers carry SEVEN repartition topics "
                        + "for the same logical key-change, each "
                        + "with full retention.ms worth of records; "
                        + "broker disk usage climbs unexplained; the "
                        + "SRE team eventually does a manual kafka-"
                        + "topics --describe sweep and discovers the "
                        + "orphans, but cannot safely delete them "
                        + "without a maintenance window because "
                        + "verifying they are truly unreferenced "
                        + "requires inspecting every deployed Streams "
                        + "app's topology; (2) full upstream replay "
                        + "on every topology edit — after a topology "
                        + "edit the new repartition topic is empty; "
                        + "the downstream groupByKey+count cannot "
                        + "resume from its changelog because its "
                        + "source (the repartition topic) is empty; "
                        + "Streams replays the entire upstream input "
                        + "through the new map node to repopulate "
                        + "the repartition topic; for a high-volume "
                        + "topic this is hours of replay during "
                        + "which the downstream count is stale; "
                        + "worse, if the upstream input has "
                        + "retention.ms shorter than the replay "
                        + "duration, you've lost data permanently; "
                        + "(3) repartition-throughput dashboards "
                        + "point at a dead topic — Grafana panels "
                        + "filtering on topic=\"app-id-KSTREAM-MAP-"
                        + "0000000005-repartition\" for produce-rate "
                        + "keep showing the old (now orphan) topic's "
                        + "metrics which decay to zero over its "
                        + "retention window; the dashboard owner "
                        + "believes throughput dropped to zero and "
                        + "pages oncall; oncall checks consumer lag, "
                        + "sees nothing wrong, dismisses the alert; "
                        + "meanwhile the actual new repartition "
                        + "topic (with a different graph-index "
                        + "suffix) is healthy but unmonitored; (4) "
                        + "map-node process-rate and record-e2e-"
                        + "latency metrics rebrand on every edit — "
                        + "the processor-node-id-tagged metrics also "
                        + "churn, breaking SLO alerts on map-node "
                        + "end-to-end latency; (5) INVOKEDYNAMIC "
                        + "method-reference captures bypass naive "
                        + "MethodInsnNode-only lint — `Function<"
                        + "KeyValueMapper<K, V, KeyValue<K2, V2>>, "
                        + "KStream<K2, V2>> rekey = stream::map` "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle "
                        + "pointing at KStream.map(KeyValueMapper)"
                        + "KStream; the user-class bytecode contains "
                        + "zero direct INVOKEINTERFACE on the "
                        + "no-Named overload, only the indy site. "
                        + "Migration: pass an explicit Named — "
                        + "`stream.map((k, v) -> KeyValue.pair("
                        + "v.customerId(), v), Named.as(\"orders-by-"
                        + "customer-rekey\"))`. The explicit Named "
                        + "name also flows into the downstream auto-"
                        + "repartition topic name, stabilizing it "
                        + "across topology edits. If no key change "
                        + "is needed, use mapValues instead — it "
                        + "does NOT insert an auto-repartition topic. "
                        + "The map overloads containing Named are "
                        + "never flagged.");
    }
}
