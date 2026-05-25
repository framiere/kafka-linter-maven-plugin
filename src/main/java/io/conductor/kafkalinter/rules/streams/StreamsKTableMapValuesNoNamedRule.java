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
 * org.apache.kafka.streams.kstream.KTable#mapValues(
 * org.apache.kafka.streams.kstream.ValueMapper) KTable.mapValues}
 * overload — any descriptor for {@code mapValues} on {@link
 * org.apache.kafka.streams.kstream.KTable KTable} that does NOT
 * include a {@link org.apache.kafka.streams.kstream.Named Named}
 * argument and therefore lets the mapValues node auto-name from
 * the topology graph index.
 *
 * <p>Predicate is structural — any descriptor for {@code
 * mapValues} on {@code KTable} whose argument list contains
 * {@code org/apache/kafka/streams/kstream/Named} is safe; any
 * descriptor that does not is unsafe.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KTable
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code table::mapValues} bound to {@link
 * java.util.function.Function
 * Function&lt;ValueMapper, KTable&gt;} or to a custom SAM whose
 * erased implMethod descriptor matches an unsafe overload).
 *
 * <h2>Why no-Named KTable.mapValues is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KTable#mapValues
 * KTable.mapValues} attaches a stateless value-projection node
 * to the topology. The node's name is auto-derived from the
 * graph index — e.g. {@code KTABLE-MAPVALUES-0000000005}. For
 * the {@code mapValues(ValueMapper, Materialized)} and {@code
 * mapValues(ValueMapperWithKey, Materialized)} overloads, the
 * Materialized's state store and changelog topic also inherit
 * the same auto-name when no Named is explicitly passed. Every
 * upstream topology edit shifts the graph index and renames
 * those artifacts.
 *
 * <h2>Why mapValues is particularly load-bearing for
 * Materialized variants</h2>
 *
 * <p>KTable.mapValues is the most common place where teams
 * intentionally materialize a derived view of an upstream
 * KTable — projecting an order envelope to an order summary,
 * extracting a status field from a denormalized record, etc.
 * The standard pattern is {@code ordersTable.mapValues(
 * Order::toSummary, Materialized.as("order-summary"))} —
 * BUT crucially, only the overload that takes BOTH Named AND
 * Materialized actually pins a stable processor-node name; if
 * you pass Materialized.as("order-summary") alone, the
 * Materialized store is correctly named but the
 * <em>processor node</em> still auto-numbers, and every metric
 * panel/runbook/trace tag keyed by node id is fragile.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Order-summary derived view restores from empty
 *       changelog after every topology edit.</b> A team has
 *       {@code ordersTable.mapValues(Order::toSummary,
 *       Materialized.as("order-summary"))}. They believe the
 *       store name is pinned. But for the version of the
 *       overload without an explicit Named on the processor
 *       node, the processor node still auto-numbers; on the
 *       next upstream {@code filter} insertion the
 *       processor-node id shifts; Streams's incremental rebuild
 *       logic detects "this is a new processor node attached to
 *       this store" and forces a full changelog replay; the
 *       order-summary KTable is unavailable to downstream joins
 *       for the duration of the replay; a downstream join
 *       backlog grows until oncall pages.</li>
 *   <li><b>Value-mapper records-processed-rate dashboards
 *       silently zero.</b> Grafana dashboard panel filters on
 *       {@code processor-node-id="KTABLE-MAPVALUES-0000000005"}
 *       for the order-summary projection's records-processed-
 *       rate. One topology edit upstream shifts the graph
 *       index; the panel reads zero; oncall stops trusting the
 *       order-summary projection's health signal; a real stuck
 *       projection (e.g. a broken {@code Order::toSummary}
 *       throwing NullPointerException on a poisoned record and
 *       being swallowed by a fault-tolerant deserializer) is
 *       invisible for days.</li>
 *   <li><b>Materialized state-store churn doubles broker
 *       disk usage.</b> {@code mapValues(mapper, Materialized.
 *       as(<auto-name>))} — without an explicit Materialized
 *       name — produces a store whose name is graph-index-
 *       derived; on every topology edit the store gets a new
 *       name; the OLD changelog topic stays on the brokers
 *       (Kafka does not auto-delete changelog topics whose
 *       store no longer exists). After three topology edits the
 *       brokers carry 4× the changelog disk volume needed; once
 *       a retention sweep purges, you've lost the ability to
 *       restore the old store should you need to roll back.</li>
 *   <li><b>OpenTelemetry value-projection spans lose
 *       continuity.</b> Each mapValues processor span carries
 *       the node id as an attribute; trace-aggregation queries
 *       grouping by node id treat the same logical mapValues
 *       step (pre- and post-topology-edit) as two unrelated
 *       processors; latency-percentile dashboards bucketize by
 *       (old, new) and the per-step p99 is split across two
 *       buckets, each appearing artificially under-loaded.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       value-projection factory built as {@code
 *       Function&lt;ValueMapper&lt;V, V2&gt;, KTable&lt;K, V2&gt;
 *       &gt; project = table::mapValues} compiles to {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle pointing at {@code
 *       KTable.mapValues(ValueMapper)KTable}. The user-class
 *       bytecode contains zero direct {@code INVOKEINTERFACE}
 *       on the no-Named overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named} naming the processor
 * node — e.g. {@code table.mapValues(Order::toSummary,
 * Named.as("order-summary-projection"))}. For materialized
 * variants, use {@code table.mapValues(Order::toSummary,
 * Named.as("order-summary-projection"),
 * Materialized.as("order-summary"))} so BOTH the processor node
 * and the backing store carry stable, user-chosen names
 * independent of the graph index.
 */
public final class StreamsKTableMapValuesNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KTABLE);
    private static final String METHOD_NAME = "mapValues";
    private static final String NAMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsKTableMapValuesNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_KTABLE_MAP_VALUES_NO_NAMED;
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
                RuleId.STREAMS_KTABLE_MAP_VALUES_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KTable.mapValues — a no-Named overload is reached "
                        + "here — either as a direct INVOKEINTERFACE "
                        + "on the method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `table::mapValues` "
                        + "bound to Function<ValueMapper, KTable> or "
                        + "to a custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). The "
                        + "mapValues node's name is auto-derived from "
                        + "the topology graph index (e.g. KTABLE-"
                        + "MAPVALUES-0000000005); for the mapValues"
                        + "(ValueMapper, Materialized) and mapValues"
                        + "(ValueMapperWithKey, Materialized) overloads "
                        + "the Materialized's state store and changelog "
                        + "topic also inherit the same auto-name when "
                        + "no Named is explicitly passed. Concrete "
                        + "failure modes: (1) order-summary derived "
                        + "view restores from empty changelog after "
                        + "every topology edit — team has ordersTable."
                        + "mapValues(Order::toSummary, Materialized.as("
                        + "\"order-summary\")) believing the store "
                        + "name is pinned, but for the overload without "
                        + "an explicit Named the processor node still "
                        + "auto-numbers; on the next upstream filter "
                        + "insertion the processor-node id shifts; "
                        + "Streams's incremental rebuild logic detects "
                        + "\"this is a new processor node attached to "
                        + "this store\" and forces a full changelog "
                        + "replay; the order-summary KTable is "
                        + "unavailable to downstream joins for the "
                        + "duration of the replay; a downstream join "
                        + "backlog grows until oncall pages; (2) value-"
                        + "mapper records-processed-rate dashboards "
                        + "silently zero — Grafana dashboard panel "
                        + "filters on processor-node-id=\"KTABLE-"
                        + "MAPVALUES-0000000005\" for the order-summary "
                        + "projection's records-processed-rate; one "
                        + "topology edit upstream shifts the graph "
                        + "index; the panel reads zero; oncall stops "
                        + "trusting the order-summary projection's "
                        + "health signal; a real stuck projection "
                        + "(e.g. a broken Order::toSummary throwing "
                        + "NullPointerException on a poisoned record "
                        + "and being swallowed by a fault-tolerant "
                        + "deserializer) is invisible for days; (3) "
                        + "materialized state-store churn doubles "
                        + "broker disk usage — mapValues(mapper, "
                        + "Materialized.as(<auto-name>)) without an "
                        + "explicit Materialized name produces a store "
                        + "whose name is graph-index-derived; on every "
                        + "topology edit the store gets a new name; "
                        + "the OLD changelog topic stays on the "
                        + "brokers (Kafka does not auto-delete "
                        + "changelog topics whose store no longer "
                        + "exists); after three topology edits the "
                        + "brokers carry 4x the changelog disk volume "
                        + "needed; once a retention sweep purges, "
                        + "you've lost the ability to restore the old "
                        + "store should you need to roll back; (4) "
                        + "OpenTelemetry value-projection spans lose "
                        + "continuity — each mapValues processor span "
                        + "carries the node id as an attribute; trace-"
                        + "aggregation queries grouping by node id "
                        + "treat the same logical mapValues step (pre- "
                        + "and post-topology-edit) as two unrelated "
                        + "processors; latency-percentile dashboards "
                        + "bucketize by (old, new) and the per-step "
                        + "p99 is split across two buckets, each "
                        + "appearing artificially under-loaded; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "`Function<ValueMapper<V, V2>, KTable<K, V2>"
                        + "> project = table::mapValues` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KTable.mapValues(ValueMapper)KTable; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit Named naming the processor node — "
                        + "`table.mapValues(Order::toSummary, Named.as("
                        + "\"order-summary-projection\"))`. For "
                        + "materialized variants, use `table.mapValues"
                        + "(Order::toSummary, Named.as(\"order-summary-"
                        + "projection\"), Materialized.as(\"order-"
                        + "summary\"))` so BOTH the processor node "
                        + "and the backing store carry stable, user-"
                        + "chosen names independent of the graph "
                        + "index. The mapValues overloads containing "
                        + "Named are never flagged.");
    }
}
