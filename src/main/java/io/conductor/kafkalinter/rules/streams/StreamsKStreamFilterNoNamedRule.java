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
 * org.apache.kafka.streams.kstream.KStream#filter(
 * org.apache.kafka.streams.kstream.Predicate) KStream.filter} or
 * {@link org.apache.kafka.streams.kstream.KStream#filterNot(
 * org.apache.kafka.streams.kstream.Predicate)
 * KStream.filterNot} overload — any descriptor for {@code
 * filter} or {@code filterNot} on {@link
 * org.apache.kafka.streams.kstream.KStream KStream} that does
 * NOT include a {@link org.apache.kafka.streams.kstream.Named
 * Named} argument and therefore lets the filter node auto-name
 * from the topology graph index.
 *
 * <p>Predicate is structural — any descriptor for {@code
 * filter} or {@code filterNot} on {@code KStream} whose
 * argument list contains {@code
 * org/apache/kafka/streams/kstream/Named} is safe; any
 * descriptor that does not is unsafe.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls
 * (KStream is an interface) and {@code INVOKEDYNAMIC}
 * method-reference captures (e.g. {@code stream::filter}
 * bound to {@link java.util.function.Function
 * Function&lt;Predicate, KStream&gt;} or to a custom SAM whose
 * erased implMethod descriptor matches an unsafe overload).
 *
 * <h2>Why no-Named KStream.filter is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#filter
 * KStream.filter} attaches a stateless filter node to the
 * topology. The node's name is auto-derived from the graph
 * index — e.g. {@code KSTREAM-FILTER-0000000003}. Unlike
 * KTable.filter, there is NO Materialized form here; the
 * hazard is purely observational — but the observation
 * surface (drop-rate alerts, per-node throughput dashboards,
 * runbook node-id references) is precisely the surface
 * oncall trusts to detect a stuck filter.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Filter-drop-rate alerts silently break on
 *       topology edits.</b> Team has {@code
 *       transactions.filter((k, v) -> !isFraudulent(v))} with
 *       a Prometheus alert: {@code rate(
 *       kafka_stream_processor_node_records_dropped{
 *       processor_node_id="KSTREAM-FILTER-0000000003"}[5m])
 *       > 50}. The alert is the primary signal for "fraud
 *       filter producing abnormal drop volume" — both
 *       (a) "too few drops" (silently letting fraud through
 *       because the predicate was broken) and (b) "too many
 *       drops" (silently dropping legitimate traffic because
 *       a feature flag flipped). One upstream {@code
 *       mapValues} insertion to enrich the record shifts the
 *       graph index from 3 to 4; the alert label no longer
 *       matches; the alert sits in "no data" state — Prometheus
 *       does not treat absent series as fired; the fraud team
 *       loses both directions of detection silently.</li>
 *   <li><b>Per-filter records-processed dashboards rebrand.</b>
 *       Grafana panels show the filter node's input-vs-output
 *       throughput as twin time-series. After a topology edit
 *       the panel reads zero on both axes (the old node id
 *       has no data; the new node id is not on the panel);
 *       oncall stops trusting it; a real stuck-filter incident
 *       is invisible until downstream consumer lag pages
 *       hours later.</li>
 *   <li><b>topology.describe() runbook drift.</b> SRE
 *       runbooks reference filter nodes by their
 *       auto-generated names — {@code "if
 *       KSTREAM-FILTER-0000000003 records-dropped-rate jumps
 *       10x in 5 minutes, page security"}. After any topology
 *       edit the referenced filter node no longer exists by
 *       that name; the runbook step silently no-ops; the
 *       operator believes they are monitoring the fraud
 *       filter but the wrong node (or no node at all) was
 *       targeted.</li>
 *   <li><b>OpenTelemetry filter-node spans lose continuity.</b>
 *       Per-record processor spans labeled with the filter
 *       node id are partitioned by (pre-edit, post-edit) id;
 *       trace queries grouping by node id treat the same
 *       logical filter as two unrelated processors; per-step
 *       latency percentiles are split across two buckets, each
 *       appearing artificially under-loaded.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       fraud-detector factory built as {@code
 *       Function&lt;Predicate&lt;K, V&gt;, KStream&lt;K, V&gt;&gt;
 *       reject = stream::filter} compiles to {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle pointing at {@code
 *       KStream.filter(Predicate)KStream}. The user-class
 *       bytecode contains zero direct {@code INVOKEINTERFACE}
 *       on the no-Named overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named} naming the filter
 * node — e.g. {@code stream.filter(predicate, Named.as(
 * "fraud-rejection-filter"))}. The Named name flows into the
 * metric tag, the runbook id, the trace span, and
 * topology.describe() output, all of which become stable
 * across upstream edits.
 */
public final class StreamsKStreamFilterNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final Set<String> METHOD_NAMES = Set.of("filter", "filterNot");
    private static final String NAMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsKStreamFilterNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_KSTREAM_FILTER_NO_NAMED;
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
                        && findUnsafeFilterHandle(indy) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private static Handle findUnsafeFilterHandle(InvokeDynamicInsnNode indy) {
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
                RuleId.STREAMS_KSTREAM_FILTER_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.filter / filterNot — a no-Named overload "
                        + "is reached here — either as a direct "
                        + "INVOKEINTERFACE on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `stream::filter` bound to "
                        + "Function<Predicate, KStream> or to a custom "
                        + "SAM whose erased implMethod descriptor "
                        + "matches an unsafe overload). The filter "
                        + "node's name is auto-derived from the "
                        + "topology graph index (e.g. KSTREAM-FILTER-"
                        + "0000000003). Concrete failure modes: (1) "
                        + "filter-drop-rate alerts silently break on "
                        + "topology edits — team has transactions."
                        + "filter((k, v) -> !isFraudulent(v)) with a "
                        + "Prometheus alert rate("
                        + "kafka_stream_processor_node_records_dropped"
                        + "{processor_node_id=\"KSTREAM-FILTER-"
                        + "0000000003\"}[5m]) > 50; the alert is the "
                        + "primary signal for both \"too few drops\" "
                        + "(silently letting fraud through because the "
                        + "predicate was broken) and \"too many "
                        + "drops\" (silently dropping legitimate "
                        + "traffic because a feature flag flipped); "
                        + "one upstream mapValues insertion to enrich "
                        + "the record shifts the graph index from 3 "
                        + "to 4; the alert label no longer matches; "
                        + "the alert sits in \"no data\" state — "
                        + "Prometheus does not treat absent series as "
                        + "fired; the fraud team loses both "
                        + "directions of detection silently; (2) per-"
                        + "filter records-processed dashboards "
                        + "rebrand — Grafana panels show the filter "
                        + "node's input-vs-output throughput as twin "
                        + "time-series; after a topology edit the "
                        + "panel reads zero on both axes (the old "
                        + "node id has no data; the new node id is "
                        + "not on the panel); oncall stops trusting "
                        + "it; a real stuck-filter incident is "
                        + "invisible until downstream consumer lag "
                        + "pages hours later; (3) topology.describe() "
                        + "runbook drift — SRE runbooks reference "
                        + "filter nodes by their auto-generated "
                        + "names (\"if KSTREAM-FILTER-0000000003 "
                        + "records-dropped-rate jumps 10x in 5 "
                        + "minutes, page security\"); after any "
                        + "topology edit the referenced filter node "
                        + "no longer exists by that name; the "
                        + "runbook step silently no-ops; the "
                        + "operator believes they are monitoring the "
                        + "fraud filter but the wrong node (or no "
                        + "node at all) was targeted; (4) "
                        + "OpenTelemetry filter-node spans lose "
                        + "continuity — per-record processor spans "
                        + "labeled with the filter node id are "
                        + "partitioned by (pre-edit, post-edit) id; "
                        + "trace queries grouping by node id treat "
                        + "the same logical filter as two unrelated "
                        + "processors; per-step latency percentiles "
                        + "are split across two buckets, each "
                        + "appearing artificially under-loaded; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "`Function<Predicate<K, V>, KStream<K, V>> "
                        + "reject = stream::filter` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KStream.filter(Predicate)KStream; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit Named naming the filter node — "
                        + "`stream.filter(predicate, Named.as("
                        + "\"fraud-rejection-filter\"))`. The Named "
                        + "name flows into the metric tag, the "
                        + "runbook id, the trace span, and "
                        + "topology.describe() output, all of which "
                        + "become stable across upstream edits. The "
                        + "filter/filterNot overloads containing "
                        + "Named are never flagged.");
    }
}
