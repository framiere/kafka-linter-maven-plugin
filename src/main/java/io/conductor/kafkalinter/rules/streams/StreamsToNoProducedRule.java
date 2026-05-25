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
 * org.apache.kafka.streams.kstream.KStream#to(String)
 * KStream.to(String)} or {@link
 * org.apache.kafka.streams.kstream.KStream#to(
 * org.apache.kafka.streams.processor.TopicNameExtractor)
 * KStream.to(TopicNameExtractor)} overload — any descriptor for
 * {@code to} on {@link org.apache.kafka.streams.kstream.KStream
 * KStream} that does NOT include a {@link
 * org.apache.kafka.streams.kstream.Produced Produced} argument
 * and therefore lets the sink node auto-name from the graph
 * index AND inherit the global default key/value serdes for
 * serialisation onto the destination topic.
 *
 * <p>Predicate is structural — any descriptor for {@code to} on
 * {@code KStream} whose argument list contains
 * {@code org/apache/kafka/streams/kstream/Produced} is safe; any
 * descriptor that does not is unsafe. This covers both unsafe
 * overload pairs:
 *
 * <ul>
 *   <li>{@code to(String)} vs {@code to(String, Produced)}</li>
 *   <li>{@code to(TopicNameExtractor)} vs
 *       {@code to(TopicNameExtractor, Produced)}</li>
 * </ul>
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code stream::to} bound to {@link
 * java.util.function.Consumer Consumer&lt;String&gt;} or to a
 * custom SAM whose erased implMethod descriptor matches an
 * unsafe overload).
 *
 * <h2>Why no-Produced to() is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#to
 * KStream.to} attaches a sink node to the topology and writes
 * each record to the destination topic. Without an explicit
 * {@link org.apache.kafka.streams.kstream.Produced} two things
 * go wrong at once:
 *
 * <ol>
 *   <li><b>Sink node name is auto-named</b> from the topology
 *       graph index — e.g. {@code KSTREAM-SINK-0000000007}.
 *       Every sink-tagged metric, every {@code
 *       topology.describe()} block, every distributed-trace
 *       producer span derives its label from that node name.</li>
 *   <li><b>Serdes default to global config</b> — {@code
 *       default.key.serde} and {@code default.value.serde} on
 *       the StreamsConfig. Any change to those globals (a
 *       config-level migration, a typo, a fleet-wide override)
 *       silently re-serialises this sink with the new serdes;
 *       if the new serdes don't match the wire format the
 *       destination topic now contains corrupt records that
 *       downstream consumers cannot deserialise.</li>
 * </ol>
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Default-serde override silently corrupts the
 *       destination topic.</b> An SRE flips {@code
 *       default.value.serde} from {@code JsonSerde} to {@code
 *       GenericAvroSerde} as part of a fleet-wide Avro
 *       migration. Every {@code .to("destination-topic")} call
 *       site that did not pin its own Produced now serialises
 *       with the Avro serde; downstream consumers (other
 *       Streams apps, batch jobs, dashboards) that still expect
 *       JSON deserialise garbage; the affected topic now has a
 *       mixed-format history that no single consumer can fully
 *       read.</li>
 *   <li><b>Sink-tagged metric panels silently empty after a
 *       topology edit.</b> A team has {@code
 *       stream.to("orders-out")} producing records tagged with
 *       sink node id {@code KSTREAM-SINK-0000000007}. Grafana
 *       panels filtered on {@code node-id="KSTREAM-SINK-
 *       0000000007"} for records-produced-rate. Team adds one
 *       upstream topology edit; the sink node index shifts; the
 *       panels read zero indefinitely; oncall stops looking; a
 *       real production-rate collapse on the orders-out sink
 *       two weeks later is invisible.</li>
 *   <li><b>{@code topology.describe()} runbook drift.</b> SRE
 *       runbooks reference sink nodes by their auto-generated
 *       names — {@code "if KSTREAM-SINK-0000000007 records-
 *       produced-rate exceeds 10k/s, throttle upstream"}. After
 *       any topology edit the referenced sink no longer exists
 *       by that name; the runbook step silently no-ops; the
 *       operator believes they have throttled the orders-out
 *       sink but the wrong sink (or no sink at all) was
 *       targeted.</li>
 *   <li><b>Distributed-trace producer spans lose continuity.</b>
 *       Kafka Streams + OpenTelemetry labels each sink-node
 *       span with the node id; trace-aggregation queries
 *       filtering by old sink-node id return zero matches after
 *       a topology edit; the APM dashboard drops the Streams
 *       app → downstream consumer correlation silently.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       sink factory built as {@code Consumer&lt;String&gt; sink
 *       = stream::to} compiles to {@code INVOKEDYNAMIC} whose
 *       bsm-args contain a {@code REF_invokeInterface} Handle
 *       pointing at {@code KStream.to(String)V}. The user-class
 *       bytecode contains zero direct {@code INVOKEINTERFACE} on
 *       the no-Produced overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Produced} pinning both
 * serdes and the sink node name — e.g. {@code stream.to(
 * "orders-out", Produced.with(Serdes.String(), orderSerde)
 * .withName("orders-out-sink"))}. The chosen name is stable
 * across topology edits because the user wrote it down; the
 * pinned serdes are impervious to global config changes.
 */
public final class StreamsToNoProducedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "to";
    private static final String PRODUCED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Produced;";

    private final Severity severity;

    public StreamsToNoProducedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_TO_NO_PRODUCED;
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
                        && !mi.desc.contains(PRODUCED_TYPE_TOKEN)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, null);
                    if (h != null && h.getDesc() != null && !h.getDesc().contains(PRODUCED_TYPE_TOKEN)) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_TO_NO_PRODUCED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.to(topic) / to(TopicNameExtractor) — a "
                        + "no-Produced overload is reached here — "
                        + "either as a direct INVOKEINTERFACE on the "
                        + "method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `stream::to` bound "
                        + "to Consumer<String> or to a custom SAM "
                        + "whose erased implMethod descriptor matches "
                        + "an unsafe overload). Without Produced, two "
                        + "things go wrong at once: the sink node is "
                        + "auto-named from the topology graph index "
                        + "(e.g. KSTREAM-SINK-0000000007) AND the "
                        + "sink's key/value serdes default to the "
                        + "global default.key.serde / "
                        + "default.value.serde on StreamsConfig. "
                        + "Concrete failure modes: (1) default-serde "
                        + "override silently corrupts the destination "
                        + "topic — an SRE flips default.value.serde "
                        + "from JsonSerde to GenericAvroSerde as part "
                        + "of a fleet-wide Avro migration; every "
                        + ".to(\"destination-topic\") call site that "
                        + "did not pin its own Produced now serialises "
                        + "with the Avro serde; downstream consumers "
                        + "(other Streams apps, batch jobs, "
                        + "dashboards) that still expect JSON "
                        + "deserialise garbage; the affected topic now "
                        + "has a mixed-format history that no single "
                        + "consumer can fully read; (2) sink-tagged "
                        + "metric panels silently empty after a "
                        + "topology edit — Grafana panels filtered on "
                        + "node-id=\"KSTREAM-SINK-0000000007\" for "
                        + "records-produced-rate; team adds one "
                        + "upstream topology edit; the sink node "
                        + "index shifts; the panels read zero "
                        + "indefinitely; oncall stops looking; a real "
                        + "production-rate collapse on the orders-out "
                        + "sink two weeks later is invisible; (3) "
                        + "topology.describe() runbook drift — SRE "
                        + "runbooks reference sink nodes by their "
                        + "auto-generated names (\"if KSTREAM-SINK-"
                        + "0000000007 records-produced-rate exceeds "
                        + "10k/s, throttle upstream\"); after any "
                        + "topology edit the referenced sink no "
                        + "longer exists by that name; the runbook "
                        + "step silently no-ops; the operator "
                        + "believes they have throttled the "
                        + "orders-out sink but the wrong sink (or no "
                        + "sink at all) was targeted; (4) "
                        + "distributed-trace producer spans lose "
                        + "continuity — Kafka Streams + OpenTelemetry "
                        + "labels each sink-node span with the node "
                        + "id; trace-aggregation queries filtering by "
                        + "old sink-node id return zero matches after "
                        + "a topology edit; the APM dashboard drops "
                        + "the Streams app → downstream consumer "
                        + "correlation silently; (5) INVOKEDYNAMIC "
                        + "method-reference captures bypass naive "
                        + "MethodInsnNode-only lint — Consumer<String> "
                        + "sink = stream::to compiles to INVOKEDYNAMIC "
                        + "whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KStream.to(String)V; the user-class "
                        + "bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Produced "
                        + "overload, only the indy site. Migration: "
                        + "pass an explicit Produced pinning both "
                        + "serdes and the sink node name — "
                        + "`stream.to(\"orders-out\", Produced.with("
                        + "Serdes.String(), orderSerde).withName("
                        + "\"orders-out-sink\"))`. The chosen name is "
                        + "stable across topology edits because the "
                        + "user wrote it down; the pinned serdes are "
                        + "impervious to global config changes. The "
                        + "two (topic|TopicNameExtractor, Produced) "
                        + "overloads are never flagged by this rule.");
    }
}
