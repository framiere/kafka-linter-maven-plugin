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
 * org.apache.kafka.streams.kstream.KTable#filter(
 * org.apache.kafka.streams.kstream.Predicate) KTable.filter} or
 * {@link org.apache.kafka.streams.kstream.KTable#filterNot(
 * org.apache.kafka.streams.kstream.Predicate) KTable.filterNot}
 * overload — any descriptor for {@code filter} or
 * {@code filterNot} on {@link
 * org.apache.kafka.streams.kstream.KTable KTable} that does NOT
 * include a {@link org.apache.kafka.streams.kstream.Named Named}
 * argument and therefore lets the filter node auto-name from the
 * topology graph index.
 *
 * <p>Predicate is structural — any descriptor for {@code filter}
 * or {@code filterNot} on {@code KTable} whose argument list
 * contains {@code org/apache/kafka/streams/kstream/Named} is
 * safe; any descriptor that does not is unsafe.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KTable
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code table::filter} bound to {@link
 * java.util.function.Function
 * Function&lt;Predicate, KTable&gt;} or to a custom SAM whose
 * erased implMethod descriptor matches an unsafe overload).
 *
 * <h2>Why no-Named KTable.filter is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KTable#filter
 * KTable.filter} attaches a stateless filter node to the
 * topology. The node's name is auto-derived from the graph
 * index — e.g. {@code KTABLE-FILTER-0000000007}. For the
 * {@code filter(Predicate, Materialized)} overload, the
 * Materialized's state store and changelog topic also inherit
 * the same auto-name when no Named is explicitly passed. Every
 * upstream topology edit shifts the graph index and renames
 * those artifacts.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Filter-tagged metric panels silently empty after a
 *       topology edit.</b> A team has {@code
 *       ordersTable.filter((k, v) -> v.isHighValue())} producing
 *       a high-value-orders KTable. Grafana panels filtered on
 *       {@code processor-node-id="KTABLE-FILTER-0000000007"} for
 *       records-processed-rate. Team adds one upstream
 *       {@code mapValues}; the graph index shifts; the panels
 *       read zero indefinitely; oncall stops looking; a real
 *       high-value-orders pipeline collapse two weeks later is
 *       invisible.</li>
 *   <li><b>Materialized state-store name churn forces a full
 *       restore on every topology edit.</b> For {@code
 *       table.filter(predicate, Materialized.as(<auto-name>))},
 *       the state store and changelog topic both auto-name from
 *       the graph index. Topology edits give the store a fresh
 *       name; Streams treats it as a new store and restores from
 *       its (now empty) changelog topic; the previous changelog
 *       topic still exists on the brokers but no instance is
 *       reading from it; downstream readers of the filtered
 *       KTable see all keys reset to "not present" until the
 *       full restore completes from upstream.</li>
 *   <li><b>{@code topology.describe()} runbook drift.</b> SRE
 *       runbooks reference filter nodes by their auto-generated
 *       names — {@code "if KTABLE-FILTER-0000000007 records-
 *       processed-rate drops below 100/s, page the data team"}.
 *       After any topology edit the referenced filter node no
 *       longer exists by that name; the runbook step silently
 *       no-ops; the operator believes they are monitoring the
 *       high-value-orders filter but the wrong node (or no
 *       node at all) was targeted.</li>
 *   <li><b>Distributed-trace processor spans lose continuity.</b>
 *       Kafka Streams + OpenTelemetry labels each filter-node
 *       span with the node id; trace-aggregation queries
 *       filtering by old filter-node id return zero matches
 *       after a topology edit; the APM dashboard drops the
 *       filter step from the per-record pipeline view
 *       silently.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       filter factory built as {@code Function&lt;Predicate
 *       &lt;K, V&gt;, KTable&lt;K, V&gt;&gt; filter =
 *       table::filter} compiles to {@code INVOKEDYNAMIC} whose
 *       bsm-args contain a {@code REF_invokeInterface} Handle
 *       pointing at {@code KTable.filter(Predicate)KTable}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Named overload, only the
 *       indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Named} naming the filter
 * node — e.g. {@code table.filter(predicate, Named.as(
 * "high-value-orders-filter"))}. For materialized variants, use
 * {@code table.filter(predicate, Named.as("..."),
 * Materialized.as("..."))} so BOTH the node and the backing
 * store carry stable, user-chosen names independent of the
 * graph index.
 */
public final class StreamsKTableFilterNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KTABLE);
    private static final Set<String> METHOD_NAMES = Set.of("filter", "filterNot");
    private static final String NAMED_TYPE_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsKTableFilterNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_KTABLE_FILTER_NO_NAMED;
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
                RuleId.STREAMS_KTABLE_FILTER_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KTable.filter / filterNot — a no-Named overload "
                        + "is reached here — either as a direct "
                        + "INVOKEINTERFACE on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `table::filter` bound to "
                        + "Function<Predicate, KTable> or to a custom "
                        + "SAM whose erased implMethod descriptor "
                        + "matches an unsafe overload). The filter "
                        + "node's name is auto-derived from the "
                        + "topology graph index (e.g. KTABLE-FILTER-"
                        + "0000000007); for the filter(Predicate, "
                        + "Materialized) overload the Materialized's "
                        + "state store and changelog topic also "
                        + "inherit the same auto-name when no Named "
                        + "is explicitly passed. Concrete failure "
                        + "modes: (1) filter-tagged metric panels "
                        + "silently empty after a topology edit — "
                        + "team has ordersTable.filter((k, v) -> "
                        + "v.isHighValue()) producing a high-value-"
                        + "orders KTable; Grafana panels filtered on "
                        + "processor-node-id=\"KTABLE-FILTER-"
                        + "0000000007\" for records-processed-rate; "
                        + "team adds one upstream mapValues; the "
                        + "graph index shifts; the panels read zero "
                        + "indefinitely; oncall stops looking; a "
                        + "real high-value-orders pipeline collapse "
                        + "two weeks later is invisible; (2) "
                        + "materialized state-store name churn forces "
                        + "a full restore on every topology edit — "
                        + "for table.filter(predicate, Materialized."
                        + "as(<auto-name>)), the state store and "
                        + "changelog topic both auto-name from the "
                        + "graph index; topology edits give the "
                        + "store a fresh name; Streams treats it as "
                        + "a new store and restores from its (now "
                        + "empty) changelog topic; the previous "
                        + "changelog topic still exists on the "
                        + "brokers but no instance is reading from "
                        + "it; downstream readers of the filtered "
                        + "KTable see all keys reset to \"not "
                        + "present\" until the full restore completes "
                        + "from upstream; (3) topology.describe() "
                        + "runbook drift — SRE runbooks reference "
                        + "filter nodes by their auto-generated "
                        + "names (\"if KTABLE-FILTER-0000000007 "
                        + "records-processed-rate drops below 100/s, "
                        + "page the data team\"); after any topology "
                        + "edit the referenced filter node no longer "
                        + "exists by that name; the runbook step "
                        + "silently no-ops; the operator believes "
                        + "they are monitoring the high-value-orders "
                        + "filter but the wrong node (or no node at "
                        + "all) was targeted; (4) distributed-trace "
                        + "processor spans lose continuity — Kafka "
                        + "Streams + OpenTelemetry labels each "
                        + "filter-node span with the node id; trace-"
                        + "aggregation queries filtering by old "
                        + "filter-node id return zero matches after "
                        + "a topology edit; the APM dashboard drops "
                        + "the filter step from the per-record "
                        + "pipeline view silently; (5) INVOKEDYNAMIC "
                        + "method-reference captures bypass naive "
                        + "MethodInsnNode-only lint — `Function<"
                        + "Predicate<K, V>, KTable<K, V>> filter = "
                        + "table::filter` compiles to INVOKEDYNAMIC "
                        + "whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KTable.filter(Predicate)KTable; the user-"
                        + "class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit Named naming the filter node — "
                        + "`table.filter(predicate, Named.as("
                        + "\"high-value-orders-filter\"))`. For "
                        + "materialized variants, use "
                        + "`table.filter(predicate, Named.as(\"...\"), "
                        + "Materialized.as(\"...\"))` so BOTH the "
                        + "node and the backing store carry stable, "
                        + "user-chosen names independent of the graph "
                        + "index. The filter/filterNot overloads "
                        + "containing Named are never flagged.");
    }
}
