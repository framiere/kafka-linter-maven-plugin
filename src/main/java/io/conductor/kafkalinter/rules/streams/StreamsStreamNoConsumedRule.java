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
 * org.apache.kafka.streams.StreamsBuilder#stream(String)
 * StreamsBuilder.stream(String)}, {@link
 * org.apache.kafka.streams.StreamsBuilder#stream(
 * java.util.Collection)
 * StreamsBuilder.stream(Collection&lt;String&gt;)} or {@link
 * org.apache.kafka.streams.StreamsBuilder#stream(
 * java.util.regex.Pattern)
 * StreamsBuilder.stream(Pattern)} overload — any descriptor for
 * {@code stream} on {@link
 * org.apache.kafka.streams.StreamsBuilder} that does NOT include
 * a {@link org.apache.kafka.streams.kstream.Consumed Consumed}
 * argument and therefore lets the source node auto-name from
 * the graph index AND inherit the global default key/value
 * serdes.
 *
 * <p>Predicate is structural — any descriptor for {@code stream}
 * on {@code StreamsBuilder} whose argument list contains
 * {@code org/apache/kafka/streams/kstream/Consumed} is safe; any
 * descriptor that does not is unsafe. This naturally covers all
 * three unsafe overload pairs:
 *
 * <ul>
 *   <li>{@code stream(String)} vs {@code stream(String, Consumed)}</li>
 *   <li>{@code stream(Collection)} vs {@code stream(Collection, Consumed)}</li>
 *   <li>{@code stream(Pattern)} vs {@code stream(Pattern, Consumed)}</li>
 * </ul>
 *
 * <p>Catches both direct {@code INVOKEVIRTUAL} calls
 * (StreamsBuilder is a class) and {@code INVOKEDYNAMIC} method-
 * reference captures (e.g. {@code builder::stream} bound to a
 * SAM whose erased implMethod descriptor matches one of the
 * unsafe overloads).
 *
 * <h2>Why no-Consumed stream() is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.StreamsBuilder#stream
 * StreamsBuilder.stream} attaches a source node to the topology.
 * Without an explicit {@link
 * org.apache.kafka.streams.kstream.Consumed} two things go wrong
 * at once:
 *
 * <ol>
 *   <li><b>Node name is auto-named</b> from the topology graph
 *       index — e.g. {@code KSTREAM-SOURCE-0000000000}. Every
 *       source-tagged metric, every {@code topology.describe()}
 *       block, every distributed-trace source span derives its
 *       label from that node name.</li>
 *   <li><b>Serdes default to global config</b> — {@code
 *       default.key.serde} and {@code default.value.serde} on
 *       the StreamsConfig. Any change to those globals (a
 *       config-level migration, a typo, a fleet-wide override)
 *       silently re-deserialises this source with the new
 *       serdes; if the new serdes don't match the wire format,
 *       the app deserialises garbage as records.</li>
 * </ol>
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Source-tagged metric panels silently empty after a
 *       topology edit.</b> A team has {@code
 *       builder.stream("orders")} producing a KStream tagged
 *       {@code KSTREAM-SOURCE-0000000000}. Grafana dashboard
 *       panels filtered on {@code node-id="KSTREAM-SOURCE-
 *       0000000000"} for records-consumed-rate and
 *       records-lag-max. Team adds one upstream topology edit
 *       elsewhere in the same StreamsBuilder; the source node
 *       index shifts to {@code KSTREAM-SOURCE-0000000002}; every
 *       panel reads zero indefinitely; oncall stops looking at
 *       those panels because they look broken; a real lag spike
 *       on the orders source two weeks later is invisible until
 *       customers complain about stale data.</li>
 *   <li><b>Default-serde override silently corrupts deserialisation
 *       on every source that did not pin its own Consumed.</b> An
 *       SRE flips {@code default.value.serde} from a
 *       {@code JsonSerde} to a {@code GenericAvroSerde} as part
 *       of a fleet-wide Avro migration. Every {@code stream(
 *       "topic")} call site that did not pin its own Consumed
 *       now deserialises with the Avro serde; topics that still
 *       carry JSON wire payloads now produce
 *       {@code SerializationException} on every record (best
 *       case) or, worse, silently mis-decode similar-looking
 *       payloads into corrupt Avro records that the downstream
 *       processor accepts.</li>
 *   <li><b>{@code topology.describe()} runbook drift.</b> SRE
 *       runbooks reference source nodes by their auto-generated
 *       names — {@code "if KSTREAM-SOURCE-0000000000 records-
 *       consumed-rate drops below 100/s, page the ingest team"}.
 *       After any topology edit the referenced source no longer
 *       exists by that name; the runbook step silently no-ops;
 *       the on-call dashboard looks normal but the alert no
 *       longer fires for the right node.</li>
 *   <li><b>Distributed-trace source spans lose continuity.</b>
 *       Kafka Streams + OpenTelemetry labels each source-node
 *       span with the node id; trace-aggregation queries
 *       filtering by old source-node id return zero matches
 *       after a topology edit; the APM dashboard drops the
 *       upstream Kafka producer → Streams app correlation
 *       silently.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures bypass
 *       naïve {@code MethodInsnNode}-only lint.</b> A source
 *       factory built as {@code Function&lt;String, KStream&lt;
 *       String, String&gt;&gt; source = builder::stream} compiles
 *       to {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} Handle pointing at {@code
 *       StreamsBuilder.stream(String)KStream}. The user-class
 *       bytecode contains zero direct {@code INVOKEVIRTUAL} on
 *       the no-Consumed overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Consumed} pinning both serdes
 * and the source node name — e.g. {@code builder.stream(
 * "orders", Consumed.with(Serdes.String(), orderSerde).withName(
 * "orders-source"))}. The chosen name is stable across topology
 * edits because the user wrote it down; the pinned serdes are
 * impervious to global config changes.
 */
public final class StreamsStreamNoConsumedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.STREAMS_BUILDER);
    private static final String METHOD_NAME = "stream";
    private static final String CONSUMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Consumed;";

    private final Severity severity;

    public StreamsStreamNoConsumedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_STREAM_NO_CONSUMED;
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
                        && !mi.desc.contains(CONSUMED_TYPE_TOKEN)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, null);
                    if (h != null && h.getDesc() != null && !h.getDesc().contains(CONSUMED_TYPE_TOKEN)) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_STREAM_NO_CONSUMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "StreamsBuilder.stream(topic) / stream(Collection) / "
                        + "stream(Pattern) — a no-Consumed overload is "
                        + "reached here — either as a direct "
                        + "INVOKEVIRTUAL on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `builder::stream` bound to "
                        + "Function<String, KStream> or to a custom "
                        + "SAM whose erased implMethod descriptor "
                        + "matches an unsafe overload). Without "
                        + "Consumed, two things go wrong at once: the "
                        + "source node is auto-named from the topology "
                        + "graph index (e.g. KSTREAM-SOURCE-0000000000) "
                        + "AND the source's key/value serdes default to "
                        + "the global default.key.serde / "
                        + "default.value.serde on StreamsConfig. "
                        + "Concrete failure modes: (1) source-tagged "
                        + "metric panels silently empty after a "
                        + "topology edit — Grafana panels filtered on "
                        + "node-id=\"KSTREAM-SOURCE-0000000000\" for "
                        + "records-consumed-rate / records-lag-max; "
                        + "team adds one upstream topology edit "
                        + "elsewhere in the same StreamsBuilder; the "
                        + "source node index shifts to KSTREAM-SOURCE-"
                        + "0000000002; every panel reads zero "
                        + "indefinitely; oncall stops looking; a real "
                        + "lag spike on the orders source two weeks "
                        + "later is invisible until customers complain "
                        + "about stale data; (2) default-serde override "
                        + "silently corrupts deserialisation on every "
                        + "source that did not pin its own Consumed — "
                        + "an SRE flips default.value.serde from a "
                        + "JsonSerde to a GenericAvroSerde as part of a "
                        + "fleet-wide Avro migration; every stream("
                        + "\"topic\") call site that did not pin its "
                        + "own Consumed now deserialises with the Avro "
                        + "serde; topics that still carry JSON wire "
                        + "payloads produce SerializationException on "
                        + "every record (best case) or, worse, silently "
                        + "mis-decode similar-looking payloads into "
                        + "corrupt Avro records that the downstream "
                        + "processor accepts; (3) topology.describe() "
                        + "runbook drift — SRE runbooks reference "
                        + "source nodes by their auto-generated names "
                        + "(\"if KSTREAM-SOURCE-0000000000 records-"
                        + "consumed-rate drops below 100/s, page the "
                        + "ingest team\"); after any topology edit the "
                        + "referenced source no longer exists by that "
                        + "name; the runbook step silently no-ops; the "
                        + "on-call dashboard looks normal but the alert "
                        + "no longer fires for the right node; (4) "
                        + "distributed-trace source spans lose "
                        + "continuity — Kafka Streams + OpenTelemetry "
                        + "labels each source-node span with the node "
                        + "id; trace-aggregation queries filtering by "
                        + "old source-node id return zero matches after "
                        + "a topology edit; the APM dashboard drops the "
                        + "upstream Kafka producer → Streams app "
                        + "correlation silently; (5) INVOKEDYNAMIC "
                        + "method-reference captures bypass naive "
                        + "MethodInsnNode-only lint — Function<String, "
                        + "KStream<String, String>> source = "
                        + "builder::stream compiles to INVOKEDYNAMIC "
                        + "whose bsm-args contain a REF_invokeVirtual "
                        + "Handle pointing at StreamsBuilder.stream("
                        + "String)KStream; the user-class bytecode "
                        + "contains zero direct INVOKEVIRTUAL on the "
                        + "no-Consumed overload, only the indy site. "
                        + "Migration: pass an explicit Consumed pinning "
                        + "both serdes and the source node name — "
                        + "`builder.stream(\"orders\", Consumed.with("
                        + "Serdes.String(), orderSerde).withName("
                        + "\"orders-source\"))`. The chosen name is "
                        + "stable across topology edits because the "
                        + "user wrote it down; the pinned serdes are "
                        + "impervious to global config changes. The "
                        + "three (topic|Collection|Pattern, Consumed) "
                        + "overloads are never flagged by this rule.");
    }
}
