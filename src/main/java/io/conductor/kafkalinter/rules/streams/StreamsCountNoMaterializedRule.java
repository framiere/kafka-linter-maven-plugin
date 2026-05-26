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
 * Fires for every reach of any overload of {@code count} on a
 * grouped-stream-like owner ({@link
 * org.apache.kafka.streams.kstream.KGroupedStream KGroupedStream},
 * {@link org.apache.kafka.streams.kstream.KGroupedTable
 * KGroupedTable}, {@link
 * org.apache.kafka.streams.kstream.TimeWindowedKStream
 * TimeWindowedKStream}, {@link
 * org.apache.kafka.streams.kstream.SessionWindowedKStream
 * SessionWindowedKStream}) whose descriptor does NOT include a
 * {@link org.apache.kafka.streams.kstream.Materialized
 * Materialized} argument.
 *
 * <p>Predicate is structural — any descriptor for {@code count}
 * on the four grouped owner interfaces whose argument list does
 * NOT contain {@code org/apache/kafka/streams/kstream/
 * Materialized} is unsafe. That captures both {@code count()}
 * and {@code count(Named)} but NOT {@code count(Materialized)}
 * or {@code count(Named, Materialized)}.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (all
 * four owners are interfaces) and {@code INVOKEDYNAMIC} method-
 * reference captures (e.g. {@code stream::count} bound to {@link
 * java.util.function.Supplier Supplier&lt;KTable&gt;}, to {@link
 * java.util.function.Function Function&lt;Named, KTable&gt;},
 * or to a custom SAM whose erased implMethod descriptor matches
 * an unsafe overload).
 *
 * <h2>Why no-Materialized {@code count()} loses the count's
 * persistent state on every topology edit</h2>
 *
 * <p>{@code count} is a stateful aggregation: every input record
 * increments a counter stored per-key in a {@link
 * org.apache.kafka.streams.state.KeyValueStore KeyValueStore}
 * (or {@link org.apache.kafka.streams.state.WindowStore
 * WindowStore} / {@link
 * org.apache.kafka.streams.state.SessionStore SessionStore} on
 * the windowed owners). When no {@link
 * org.apache.kafka.streams.kstream.Materialized Materialized} is
 * passed, the state store and its companion changelog topic
 * are auto-named from the topology graph index — e.g. {@code
 * KSTREAM-AGGREGATE-STATE-STORE-0000000007} and {@code app-id-
 * KSTREAM-AGGREGATE-STATE-STORE-0000000007-changelog}.
 *
 * <p>{@code count(Named)} is a half-fix: {@link
 * org.apache.kafka.streams.kstream.Named Named} pins the
 * processor-node id and the JMX MBean names, but NOT the state-
 * store name or the changelog topic name — the state-store name
 * is derived from {@link
 * org.apache.kafka.streams.kstream.Materialized#as(String)
 * Materialized.as(String)}, not from {@code Named}. So {@code
 * count(Named.as("dau-counter"))} still leaves the state store
 * and changelog graph-index-derived, and the topology-edit-rename
 * hazard persists.
 *
 * <p>The combination required for full stability is BOTH {@link
 * org.apache.kafka.streams.kstream.Named Named} (for the
 * processor-node id) AND {@link
 * org.apache.kafka.streams.kstream.Materialized Materialized}
 * (for the state store and changelog). This rule covers the
 * Materialized half; {@code STREAMS_COUNT_NO_NAMED} covers the
 * Named half. Both must be passed for full coverage.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>The daily-active-user counter silently restarts from
 *       zero.</b> Team has {@code clicks.groupByKey().count()}
 *       — the DAU counter. After one upstream topology edit the
 *       graph index shifts; Streams creates a NEW state store
 *       and a NEW changelog topic (both empty); the counter
 *       restarts from zero on the next deploy; the DAU
 *       dashboard shows a step-discontinuity that takes hours
 *       of investigation to attribute. The bug is silent
 *       (no exception, no warning log) because Streams treats
 *       an empty state store as a valid initial state.</li>
 *   <li><b>{@code count(Named)} is a half-fix that hides the
 *       problem.</b> A team that read a blog post on "name your
 *       Streams operators" added {@code count(Named.as("dau-
 *       counter"))} everywhere. The processor node name is now
 *       stable; the JMX MBean {@code kafka.streams:type=
 *       stream-processor-node-metrics,processor-node-id=dau-
 *       counter} works; the operator believes the counter is
 *       fully named. But the state store and changelog topic
 *       are STILL graph-index-derived; the topology-edit-
 *       rename hazard is still active; the next topology edit
 *       still silently restarts the counter from zero. The
 *       false sense of safety makes the eventual incident
 *       worse: SRE explicitly checked that the operator was
 *       named.</li>
 *   <li><b>State-store changelog orphan accumulation on broker
 *       disk.</b> Each topology edit leaves the previous count's
 *       changelog topic ({@code app-id-KSTREAM-AGGREGATE-STATE-
 *       STORE-0000000007-changelog}) orphaned with full retention
 *       (Streams does not auto-delete changelog topics whose
 *       producer no longer references them; the topic name is
 *       now off-graph). For per-key counts the changelog grows
 *       at the input throughput rate; an org with 50 counters
 *       and 4 topology edits per counter per year accumulates
 *       200 orphan changelog topics per year, each carrying
 *       per-key state for its full retention window.</li>
 *   <li><b>Grafana count-throughput panels rebrand on every
 *       edit.</b> Panels filter on {@code topic="app-id-
 *       KSTREAM-AGGREGATE-STATE-STORE-0000000007-changelog"}
 *       for changelog-write-rate and changelog-bytes-in-per-sec.
 *       After any topology edit the panel reads zero; the
 *       counter looks like it stopped counting; the operator
 *       investigates an aggregator they cannot find in
 *       topology.describe() because its name has shifted.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       count factory built as {@code Supplier&lt;KTable&lt;K,
 *       Long&gt;&gt; counter = grouped::count} compiles to an
 *       {@code INVOKEDYNAMIC} site whose bsm-args contain a
 *       {@code REF_invokeInterface} Handle pointing at {@code
 *       KGroupedStream.count()KTable}. The user-class bytecode
 *       contains zero direct {@code INVOKEINTERFACE} on the
 *       no-Materialized overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Materialized Materialized}
 * naming the state store — e.g. {@code grouped.count(
 * Materialized.as("dau-counter-store"))}. The explicit name pins
 * BOTH the state store name AND the changelog topic name ({@code
 * app-id-dau-counter-store-changelog}). For full naming coverage,
 * pair {@code Materialized.as(...)} with {@code Named.as(...)}
 * (the 4-arg {@code count(Named, Materialized)} overload).
 */
public final class StreamsCountNoMaterializedRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.GROUPED_KSTREAM_OWNERS;
    private static final String METHOD_NAME = "count";
    private static final String MATERIALIZED_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Materialized;";

    private final Severity severity;

    public StreamsCountNoMaterializedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_COUNT_NO_MATERIALIZED;
    }

    private static boolean isUnsafe(String desc) {
        return desc != null && !desc.contains(MATERIALIZED_TOKEN);
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && isUnsafe(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, null);
                    if (h != null && isUnsafe(h.getDesc())) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_COUNT_NO_MATERIALIZED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KGroupedStream / KGroupedTable / TimeWindowed"
                        + "KStream / SessionWindowedKStream . count "
                        + "— a no-Materialized overload is reached "
                        + "here (either count() or count(Named)) — "
                        + "either as a direct INVOKEINTERFACE on the "
                        + "method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `grouped::count` "
                        + "bound to Supplier<KTable>, Function<Named, "
                        + "KTable>, or to a custom SAM whose erased "
                        + "implMethod descriptor matches an unsafe "
                        + "overload). count is a stateful "
                        + "aggregation: every input record increments "
                        + "a counter stored per-key in a state store. "
                        + "When no Materialized is passed, the state "
                        + "store AND its companion changelog topic "
                        + "are auto-named from the topology graph "
                        + "index (e.g. KSTREAM-AGGREGATE-STATE-STORE-"
                        + "0000000007 and app-id-KSTREAM-AGGREGATE-"
                        + "STATE-STORE-0000000007-changelog). "
                        + "count(Named) is a HALF-FIX: Named pins "
                        + "the processor-node id and JMX MBean names "
                        + "but NOT the state-store name or the "
                        + "changelog topic name (the state-store "
                        + "name is derived from Materialized.as("
                        + "String), not from Named). So count(Named"
                        + ".as(\"dau-counter\")) still leaves the "
                        + "state store and changelog graph-index-"
                        + "derived, and the topology-edit-rename "
                        + "hazard persists. The combination required "
                        + "for full stability is BOTH Named (for the "
                        + "processor-node id) AND Materialized (for "
                        + "the state store and changelog) — pair "
                        + "this rule with STREAMS_COUNT_NO_NAMED. "
                        + "Concrete failure modes: (1) the daily-"
                        + "active-user counter silently restarts "
                        + "from zero — team has clicks.groupByKey()"
                        + ".count() (the DAU counter); after one "
                        + "upstream topology edit the graph index "
                        + "shifts, Streams creates a NEW state store "
                        + "and a NEW changelog topic (both empty), "
                        + "the counter restarts from zero on the "
                        + "next deploy, the DAU dashboard shows a "
                        + "step-discontinuity that takes hours of "
                        + "investigation; the bug is silent (no "
                        + "exception, no warning log) because "
                        + "Streams treats an empty state store as a "
                        + "valid initial state; (2) count(Named) is "
                        + "a half-fix that HIDES the problem — a "
                        + "team that read a blog post on \"name your "
                        + "Streams operators\" added count(Named.as("
                        + "\"dau-counter\")) everywhere; the "
                        + "processor node name is now stable, the "
                        + "JMX MBean kafka.streams:type=stream-"
                        + "processor-node-metrics,processor-node-id="
                        + "dau-counter works, the operator believes "
                        + "the counter is fully named; but the "
                        + "state store and changelog topic are "
                        + "STILL graph-index-derived, the topology-"
                        + "edit-rename hazard is still active, the "
                        + "next topology edit still silently "
                        + "restarts the counter from zero; the "
                        + "false sense of safety makes the eventual "
                        + "incident WORSE because SRE explicitly "
                        + "checked that the operator was named; "
                        + "(3) state-store changelog orphan "
                        + "accumulation on broker disk — each "
                        + "topology edit leaves the previous "
                        + "count's changelog topic (app-id-KSTREAM-"
                        + "AGGREGATE-STATE-STORE-0000000007-"
                        + "changelog) orphaned with full retention; "
                        + "Streams does not auto-delete changelog "
                        + "topics whose producer no longer "
                        + "references them; for per-key counts the "
                        + "changelog grows at the input throughput "
                        + "rate; an org with 50 counters and 4 "
                        + "topology edits per counter per year "
                        + "accumulates 200 orphan changelog topics "
                        + "per year, each carrying per-key state "
                        + "for its full retention window; (4) "
                        + "Grafana count-throughput panels rebrand "
                        + "on every edit — panels filter on topic="
                        + "\"app-id-KSTREAM-AGGREGATE-STATE-STORE-"
                        + "0000000007-changelog\" for changelog-"
                        + "write-rate and changelog-bytes-in-per-"
                        + "sec; after any topology edit the panel "
                        + "reads zero, the counter looks like it "
                        + "stopped counting, the operator "
                        + "investigates an aggregator they cannot "
                        + "find in topology.describe() because its "
                        + "name has shifted; (5) INVOKEDYNAMIC "
                        + "method-reference captures bypass naive "
                        + "MethodInsnNode-only lint — `Supplier<"
                        + "KTable<K, Long>> counter = grouped::"
                        + "count` compiles to INVOKEDYNAMIC whose "
                        + "bsm-args contain a REF_invokeInterface "
                        + "Handle pointing at KGroupedStream.count()"
                        + "KTable; the user-class bytecode contains "
                        + "zero direct INVOKEINTERFACE on the no-"
                        + "Materialized overload, only the indy "
                        + "site. Migration: pass an explicit "
                        + "Materialized naming the state store — "
                        + "`grouped.count(Materialized.as(\"dau-"
                        + "counter-store\"))`. The explicit name "
                        + "pins BOTH the state store name AND the "
                        + "changelog topic name (app-id-dau-counter-"
                        + "store-changelog). For full naming "
                        + "coverage, pair Materialized.as(...) with "
                        + "Named.as(...) — the 4-arg count(Named, "
                        + "Materialized) overload. The count "
                        + "overloads containing Materialized are "
                        + "never flagged.");
    }
}
