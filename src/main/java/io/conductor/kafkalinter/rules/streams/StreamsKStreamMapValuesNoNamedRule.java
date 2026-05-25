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
 * org.apache.kafka.streams.kstream.KStream#mapValues(
 * org.apache.kafka.streams.kstream.ValueMapper)
 * KStream.mapValues} overload — any descriptor for {@code
 * mapValues} on {@link org.apache.kafka.streams.kstream.KStream
 * KStream} that does NOT include a {@link
 * org.apache.kafka.streams.kstream.Named Named} argument and
 * therefore lets the mapValues node auto-name from the
 * topology graph index.
 *
 * <p>Predicate is structural — any descriptor for {@code
 * mapValues} on {@code KStream} whose argument list contains
 * {@code org/apache/kafka/streams/kstream/Named} is safe; any
 * descriptor that does not is unsafe.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls
 * (KStream is an interface) and {@code INVOKEDYNAMIC}
 * method-reference captures (e.g. {@code stream::mapValues}
 * bound to {@link java.util.function.Function
 * Function&lt;ValueMapper, KStream&gt;} or to a custom SAM
 * whose erased implMethod descriptor matches an unsafe
 * overload).
 *
 * <h2>Why no-Named KStream.mapValues is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#mapValues
 * KStream.mapValues} attaches a stateless value-projection
 * node to the topology. The node's name is auto-derived from
 * the graph index — e.g. {@code KSTREAM-MAPVALUES-0000000004}.
 *
 * <p>Critically, {@code mapValues} is the <em>safe alternative
 * to {@code map}</em> for purely-value transformations because
 * it preserves the partitioning key — there is NO auto-
 * repartition hazard. This is the precise reason teams pick
 * mapValues over map; but without an explicit Named the
 * observability hazard (per-node metrics, alert labels, trace
 * spans, runbook ids) is identical to that of map. Teams that
 * carefully chose mapValues for partition stability still pay
 * the full graph-index-rename cost on every topology edit.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Enrichment-throughput dashboards silently zero
 *       after topology edits.</b> Team has {@code
 *       orders.mapValues(Order::enrichWithCustomerProfile)}
 *       — an in-process enrichment that joins to a static
 *       in-memory profile cache. Grafana panel filters on
 *       {@code processor-node-id="KSTREAM-MAPVALUES-
 *       0000000004"} for records-processed-rate. One upstream
 *       filter insertion shifts the graph index; the panel
 *       reads zero indefinitely; a real enrichment outage
 *       (e.g. the profile cache fails to refresh because the
 *       refresh thread silently died) is invisible until
 *       downstream consumers detect missing enrichment fields
 *       hours later.</li>
 *   <li><b>Per-mapValues p99 latency alerts silently
 *       break.</b> Alerting on
 *       {@code histogram_quantile(0.99, sum(rate(
 *       kafka_stream_processor_node_process_latency_bucket{
 *       processor_node_id="KSTREAM-MAPVALUES-0000000004"
 *       }[5m])) by (le)) > 50ms} catches a slow enrichment
 *       (e.g. profile lookup degraded by GC pressure). After a
 *       topology edit the alert label no longer matches; the
 *       alert sits in "no data"; the SLO breach manifests
 *       downstream as elevated end-to-end latency without an
 *       obvious culprit.</li>
 *   <li><b>topology.describe() runbook drift.</b> SRE runbooks
 *       reference mapValues nodes by their auto-generated
 *       names — {@code "if KSTREAM-MAPVALUES-0000000004
 *       process-error-rate exceeds 1%, restart the profile
 *       cache refresh thread"}. After any topology edit the
 *       referenced node no longer exists by that name; the
 *       runbook step silently no-ops; the operator believes
 *       they are operating on the enrichment node but the
 *       wrong node (or no node at all) was targeted.</li>
 *   <li><b>OpenTelemetry enrichment spans lose continuity.</b>
 *       Per-record processor spans labeled with the mapValues
 *       node id are partitioned by (pre-edit, post-edit) id;
 *       trace queries grouping by node id treat the same
 *       logical enrichment as two unrelated processors;
 *       latency-percentile dashboards bucketize by (old, new)
 *       and the per-step p99 is split across two buckets,
 *       each appearing artificially under-loaded.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> An
 *       enrichment-factory built as {@code Function&lt;
 *       ValueMapper&lt;V, V2&gt;, KStream&lt;K, V2&gt;&gt;
 *       enrich = stream::mapValues} compiles to {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle pointing at {@code
 *       KStream.mapValues(ValueMapper)KStream}. The user-class
 *       bytecode contains zero direct {@code INVOKEINTERFACE}
 *       on the no-Named overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named} naming the mapValues
 * node — e.g. {@code stream.mapValues(
 * Order::enrichWithCustomerProfile, Named.as(
 * "order-profile-enrichment"))}. The Named name flows into
 * the metric tag, the runbook id, the trace span, and
 * topology.describe() output, all of which become stable
 * across upstream edits.
 */
public final class StreamsKStreamMapValuesNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "mapValues";
    private static final String NAMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsKStreamMapValuesNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_KSTREAM_MAP_VALUES_NO_NAMED;
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
                RuleId.STREAMS_KSTREAM_MAP_VALUES_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.mapValues — a no-Named overload is "
                        + "reached here — either as a direct "
                        + "INVOKEINTERFACE on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `stream::mapValues` bound to "
                        + "Function<ValueMapper, KStream> or to a "
                        + "custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). "
                        + "The mapValues node's name is auto-derived "
                        + "from the topology graph index (e.g. "
                        + "KSTREAM-MAPVALUES-0000000004). mapValues "
                        + "is the safe alternative to map for purely-"
                        + "value transformations because it preserves "
                        + "the partitioning key — there is no auto-"
                        + "repartition hazard. This is the precise "
                        + "reason teams pick mapValues over map; but "
                        + "without an explicit Named the observability "
                        + "hazard is identical. Concrete failure "
                        + "modes: (1) enrichment-throughput dashboards "
                        + "silently zero after topology edits — team "
                        + "has orders.mapValues(Order::"
                        + "enrichWithCustomerProfile), an in-process "
                        + "enrichment that joins to a static in-memory "
                        + "profile cache; Grafana panel filters on "
                        + "processor-node-id=\"KSTREAM-MAPVALUES-"
                        + "0000000004\" for records-processed-rate; "
                        + "one upstream filter insertion shifts the "
                        + "graph index; the panel reads zero "
                        + "indefinitely; a real enrichment outage "
                        + "(e.g. the profile cache fails to refresh "
                        + "because the refresh thread silently died) "
                        + "is invisible until downstream consumers "
                        + "detect missing enrichment fields hours "
                        + "later; (2) per-mapValues p99 latency "
                        + "alerts silently break — alerting on "
                        + "histogram_quantile(0.99, sum(rate("
                        + "kafka_stream_processor_node_process_latency"
                        + "_bucket{processor_node_id=\"KSTREAM-"
                        + "MAPVALUES-0000000004\"}[5m])) by (le)) > "
                        + "50ms catches a slow enrichment (e.g. "
                        + "profile lookup degraded by GC pressure); "
                        + "after a topology edit the alert label no "
                        + "longer matches; the alert sits in \"no "
                        + "data\"; the SLO breach manifests "
                        + "downstream as elevated end-to-end latency "
                        + "without an obvious culprit; (3) "
                        + "topology.describe() runbook drift — SRE "
                        + "runbooks reference mapValues nodes by "
                        + "their auto-generated names (\"if KSTREAM-"
                        + "MAPVALUES-0000000004 process-error-rate "
                        + "exceeds 1%, restart the profile cache "
                        + "refresh thread\"); after any topology edit "
                        + "the referenced node no longer exists by "
                        + "that name; the runbook step silently no-"
                        + "ops; (4) OpenTelemetry enrichment spans "
                        + "lose continuity — per-record processor "
                        + "spans labeled with the mapValues node id "
                        + "are partitioned by (pre-edit, post-edit) "
                        + "id; trace queries grouping by node id "
                        + "treat the same logical enrichment as two "
                        + "unrelated processors; latency-percentile "
                        + "dashboards bucketize by (old, new) and "
                        + "the per-step p99 is split across two "
                        + "buckets, each appearing artificially "
                        + "under-loaded; (5) INVOKEDYNAMIC method-"
                        + "reference captures bypass naive "
                        + "MethodInsnNode-only lint — `Function<"
                        + "ValueMapper<V, V2>, KStream<K, V2>> enrich "
                        + "= stream::mapValues` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KStream.mapValues(ValueMapper)KStream; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit Named naming the mapValues node "
                        + "— `stream.mapValues(Order::"
                        + "enrichWithCustomerProfile, Named.as("
                        + "\"order-profile-enrichment\"))`. The "
                        + "mapValues overloads containing Named are "
                        + "never flagged.");
    }
}
