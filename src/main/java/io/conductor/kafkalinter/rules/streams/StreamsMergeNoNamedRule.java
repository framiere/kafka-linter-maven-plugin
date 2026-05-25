package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of the no-Named overload of {@link
 * org.apache.kafka.streams.kstream.KStream#merge(
 * org.apache.kafka.streams.kstream.KStream) KStream.merge(KStream)}
 * — the variant that does NOT take a {@link
 * org.apache.kafka.streams.kstream.Named Named} argument and
 * therefore leaves the merge processor node auto-named from the
 * topology graph index.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code primary::merge} bound to a {@link
 * java.util.function.Function Function&lt;KStream, KStream&gt;}
 * or to a custom SAM whose erased implMethod descriptor matches
 * the unsafe overload exactly).
 *
 * <h2>Why no-Named merge is a metrics-/observability hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#merge merge}
 * unifies two upstream KStreams into one. The merge itself is
 * stateless — no repartition topic, no state store — so the
 * blast radius is narrower than {@code selectKey} or
 * {@code groupBy}. But the merge introduces a new processor node
 * in the topology, and that node carries:
 *
 * <ul>
 *   <li><b>Per-processor-node JMX metrics.</b> Streams emits
 *       per-node metrics tagged by node ID — e.g.
 *       {@code processor-node-id=KSTREAM-MERGE-0000000005},
 *       {@code process-rate}, {@code process-latency-avg},
 *       {@code dropped-records-rate}. Dashboards in Grafana /
 *       Datadog / Prometheus that scope panels by the node ID
 *       silently lose data on every topology edit, because the
 *       node ID is auto-derived from the topology graph index.</li>
 *   <li><b>Per-node trace spans / structured-log node-name
 *       fields.</b> Custom processor APIs that include the node
 *       name in trace spans (e.g. via OpenTelemetry processor
 *       instrumentation) or in structured log fields lose
 *       continuity with historical traces / logs whenever the
 *       node ID shifts.</li>
 *   <li><b>topology.describe() output</b> in operational tooling
 *       that copy-pastes the textual topology description into
 *       runbooks ages instantly on every graph-index shift.</li>
 * </ul>
 *
 * <p>Without an explicit {@link
 * org.apache.kafka.streams.kstream.Named#as(String)}, any upstream
 * topology change above either side of the merge shifts the
 * sequence counter and renames the merge node.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Per-node metric panels silently empty after deploy.</b>
 *       A team has a Grafana dashboard panel that filters
 *       {@code kafka_stream_processor_node_process_total} by
 *       {@code node_id="KSTREAM-MERGE-0000000005"} — the
 *       throughput of the merged primary+secondary input streams.
 *       Engineers ship a hotfix that adds a {@code filter} above
 *       one of the merge inputs. On deploy the merge node's graph
 *       index shifts (e.g. to 0000000007); the Grafana panel
 *       silently shows zero throughput; on-call sees "merge
 *       broken" and pages the team; team eventually discovers
 *       the metric label changed; updates the dashboard; loses
 *       40 minutes of investigation time.</li>
 *   <li><b>topology.describe() runbook drift.</b> Ops captures
 *       the textual output of
 *       {@code topology.describe().toString()} into a runbook
 *       page ("how to reason about this topology"). The runbook
 *       references {@code KSTREAM-MERGE-0000000005} explicitly
 *       in its diagrams. On every topology edit the node IDs
 *       shift; the runbook gradually becomes a fiction; an
 *       on-call engineer trying to chase a hang follows the
 *       runbook to the wrong node and misdiagnoses.</li>
 *   <li><b>OpenTelemetry processor trace spans lose continuity.</b>
 *       A custom processor-API instrumentation tags spans with
 *       the processor node name. After a topology edit, the new
 *       merge node has a new name; historical traces with the
 *       old name don't surface in the same Jaeger / Tempo trace
 *       query as the new traces; SRE building a histogram of
 *       merge latency over time sees a discontinuity at every
 *       deploy.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures bypass
 *       naïve {@code MethodInsnNode}-only lint.</b> A reroute
 *       factory built as {@code Function<KStream<String, Event>,
 *       KStream<String, Event>> merger = primary::merge}
 *       compiles to {@code INVOKEDYNAMIC} whose bsm-args contain
 *       a {@code REF_invokeInterface} Handle pointing at
 *       {@code KStream.merge(KStream)KStream} — the unsafe
 *       descriptor exactly. The user-class bytecode contains
 *       zero direct {@code INVOKEINTERFACE} on the no-Named
 *       overload, only the indy site.</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the one unsafe overload
 * descriptor. Migration: pass a stable Named argument —
 * {@code primary.merge(secondary, Named.as("merge-primary-
 * secondary"))}. The chosen node name must be stable across
 * topology edits because it carries per-node metric tags, trace
 * span names, and runbook references. The safe overload (with
 * Named) is never flagged by this rule.
 */
public final class StreamsMergeNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "merge";
    private static final String UNSAFE_DESC =
            "(Lorg/apache/kafka/streams/kstream/KStream;)"
                    + "Lorg/apache/kafka/streams/kstream/KStream;";

    private final Severity severity;

    public StreamsMergeNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_MERGE_NO_NAMED;
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
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, UNSAFE_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_MERGE_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.merge(KStream) — the no-Named overload is "
                        + "reached here — either as a direct "
                        + "INVOKEINTERFACE on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`primary::merge` bound to a SAM whose erased "
                        + "implMethod descriptor matches the unsafe "
                        + "overload). merge is stateless — no "
                        + "repartition topic, no state store — but it "
                        + "still introduces a processor node in the "
                        + "topology, and that node carries per-node JMX "
                        + "metrics (e.g. `processor-node-id=KSTREAM-"
                        + "MERGE-0000000005`, process-rate, "
                        + "process-latency-avg, dropped-records-rate), "
                        + "per-node trace spans / structured-log node "
                        + "fields, and lines in `topology.describe()` "
                        + "output. Without an explicit Named, any "
                        + "upstream topology change above either side "
                        + "of the merge shifts the sequence counter and "
                        + "renames the node. Concrete failure modes: "
                        + "(1) per-node metric panels silently empty "
                        + "after deploy — Grafana panel filters "
                        + "`kafka_stream_processor_node_process_total` "
                        + "by `node_id=\"KSTREAM-MERGE-0000000005\"` for "
                        + "the merged primary+secondary throughput; team "
                        + "ships a hotfix adding one filter above one "
                        + "merge input; graph index shifts (e.g. to "
                        + "0000000007); Grafana panel silently shows "
                        + "zero throughput; on-call sees `merge broken` "
                        + "and pages; team eventually discovers the "
                        + "metric label changed; loses 40 minutes of "
                        + "investigation time; (2) topology.describe() "
                        + "runbook drift — ops captures the textual "
                        + "output of `topology.describe().toString()` "
                        + "into a runbook page that references "
                        + "`KSTREAM-MERGE-0000000005` explicitly in its "
                        + "diagrams; on every topology edit the node "
                        + "IDs shift; the runbook gradually becomes a "
                        + "fiction; an on-call engineer trying to chase "
                        + "a hang follows the runbook to the wrong node "
                        + "and misdiagnoses; (3) OpenTelemetry "
                        + "processor trace spans lose continuity — "
                        + "custom processor-API instrumentation tags "
                        + "spans with the processor node name; after a "
                        + "topology edit, the new merge node has a new "
                        + "name; historical traces with the old name "
                        + "don't surface in the same Jaeger/Tempo trace "
                        + "query as the new traces; SRE building a "
                        + "histogram of merge latency over time sees a "
                        + "discontinuity at every deploy; (4) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "`Function<KStream<String, Event>, KStream<"
                        + "String, Event>> merger = primary::merge` "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle "
                        + "pointing at KStream.merge(KStream)KStream; "
                        + "the user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass a stable "
                        + "Named argument — `primary.merge(secondary, "
                        + "Named.as(\"merge-primary-secondary\"))`. The "
                        + "chosen node name must be stable across "
                        + "topology edits because it carries per-node "
                        + "metric tags, trace span names, and runbook "
                        + "references. The safe overload (with Named) "
                        + "is never flagged by this rule.");
    }
}
