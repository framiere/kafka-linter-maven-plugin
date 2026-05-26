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
 * Fires for every reach of any overload of {@code aggregate} on
 * a grouped-stream-like owner ({@link
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
 * <p>Predicate is structural — any descriptor for {@code
 * aggregate} on the four grouped owner interfaces whose argument
 * list does NOT contain {@code org/apache/kafka/streams/kstream/
 * Materialized} is unsafe. That captures the canonical
 * {@code aggregate(Initializer, Aggregator)} overload (and on
 * {@link org.apache.kafka.streams.kstream.KGroupedTable
 * KGroupedTable} the {@code aggregate(Initializer, Aggregator,
 * Aggregator)} adder-subtractor form, and on {@link
 * org.apache.kafka.streams.kstream.SessionWindowedKStream
 * SessionWindowedKStream} the {@code aggregate(Initializer,
 * Aggregator, Merger)} form), but NOT any overload that includes
 * a {@code Materialized} argument. Unlike {@code count}, the
 * Kafka API does NOT expose a standalone {@code Named}-only
 * half-fix for {@code aggregate} — every Named overload is
 * paired with Materialized — so the rule only needs to filter on
 * Materialized presence.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (all
 * four owners are interfaces) and {@code INVOKEDYNAMIC} method-
 * reference captures (e.g. {@code grouped::aggregate} bound to
 * a custom SAM whose erased implMethod descriptor matches an
 * unsafe overload).
 *
 * <h2>Why aggregate is THE most-expensive-to-rebuild aggregation
 * — losing its state on every topology edit hurts the most</h2>
 *
 * <p>Of the three aggregations ({@code count}, {@code reduce},
 * {@code aggregate}), {@code aggregate} carries the LARGEST per-
 * key state: a custom user-defined accumulator type {@code VR}
 * which can be an arbitrarily-complex record (typically a
 * frame-windowed snapshot, a running quantile sketch, a
 * cardinality estimator, a maintained join-result row, etc.).
 * {@code count} is just a {@code Long}; {@code reduce} replaces
 * one value with another of the same type; only {@code
 * aggregate} maintains custom-typed running state that can be
 * substantially larger than any single input record.
 *
 * <p>When no {@link
 * org.apache.kafka.streams.kstream.Materialized Materialized} is
 * passed, the state store AND its companion changelog topic are
 * auto-named from the topology graph index — e.g. {@code
 * KSTREAM-AGGREGATE-STATE-STORE-0000000007} and {@code app-id-
 * KSTREAM-AGGREGATE-STATE-STORE-0000000007-changelog}. Any
 * topology edit upstream renames the state store and the
 * changelog; the new state store starts empty (re-initialized
 * from the {@link
 * org.apache.kafka.streams.kstream.Initializer Initializer}'s
 * seed value, not from the changelog because the changelog has
 * also been renamed); the OLD changelog topic is orphaned on
 * broker disk with full retention.
 *
 * <p>Because {@code aggregate}'s state can be substantially
 * larger than {@code count}'s (custom VR record vs. {@code
 * Long}), the orphaned changelog disk footprint is
 * proportionally larger too — typically 50× to 500× the volume
 * of an orphaned {@code count} changelog at the same input
 * throughput.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Running quantile sketch is lost; SLO dashboards
 *       lie.</b> Team has {@code requests.groupByKey()
 *       .aggregate(QuantileSketch::new, (k, v, acc) -> acc.add
 *       (v.latency()))} — a per-service running p99 latency
 *       sketch backing the SLO dashboard. After one upstream
 *       topology edit the graph index shifts; Streams creates a
 *       NEW state store and a NEW changelog topic (both empty);
 *       the sketches reset to the Initializer's seed (an empty
 *       sketch); the p99 latency reads near-zero for the warm-up
 *       window because an empty sketch is small until populated;
 *       the SLO dashboard goes green; the SRE team that was
 *       considering a paging-rule loosen looks at the dashboard
 *       and proceeds; real latency was actually elevated during
 *       this window but masked by the seed-reset.</li>
 *   <li><b>Custom-VR changelog orphans carry 50x-500x the disk
 *       footprint of a count orphan.</b> Each topology edit
 *       leaves the previous aggregate's changelog ({@code app-
 *       id-KSTREAM-AGGREGATE-STATE-STORE-0000000007-changelog})
 *       orphaned with full retention. For an aggregate
 *       maintaining a 4 KB sketch per key against 1k keys and
 *       1k updates/sec, the changelog runs at ~4 MB/sec —
 *       72 GB per active day — vs. a count changelog at 1k
 *       updates/sec carrying 8-byte longs ({@code ~8 KB/sec},
 *       148 MB per active day). An org with 20
 *       aggregate(no-Materialized) call sites and 4 topology
 *       edits per site per year accumulates ~20 * 4 = 80 orphan
 *       custom-VR changelog topics per year, each carrying full
 *       retention. That is the canonical "Kafka cluster is
 *       running out of disk and nobody knows why" incident.</li>
 *   <li><b>Multi-owner coverage matters: KGroupedTable.aggregate
 *       (Initializer, Aggregator, Aggregator) (adder +
 *       subtractor) carries the same hazard.</b> KTable
 *       aggregations track NET state per key — every input
 *       record runs through both the adder AND the subtractor
 *       of the prior value. Custom-VR running state on the
 *       KGroupedTable path (typically a maintained join-row or
 *       a derived denormalized record) is even more expensive
 *       to lose than the KGroupedStream case because rebuilding
 *       it requires replaying BOTH the upstream KTable's
 *       changelog and re-running the subtract-then-add
 *       transition for every key — a 2x replay cost at minimum.
 *       This rule covers all four grouped owners
 *       (KGroupedStream, KGroupedTable, TimeWindowedKStream,
 *       SessionWindowedKStream) uniformly.</li>
 *   <li><b>Stateful-restore time blows up after every edit.</b>
 *       Even if the team eventually realises the sketches reset
 *       and recovers by re-deriving them from a source topic
 *       replay, the recovery cost for a custom-VR aggregator is
 *       proportional to the input volume × the topology's
 *       retention window — a 7-day SLO dashboard backed by a
 *       p99 sketch may need a 7-day input replay to reach
 *       steady state. The Streams instances handle 2x throughput
 *       (input + rebuild) for the full replay window.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> An
 *       aggregate factory built as a custom 2-arg SAM
 *       {@code AggFactory<K, V, VR>} bound to {@code
 *       grouped::aggregate} compiles to {@code INVOKEDYNAMIC}
 *       whose bsm-args contain a {@code REF_invokeInterface}
 *       Handle pointing at {@code KGroupedStream.aggregate(
 *       Initializer, Aggregator)KTable}. The user-class bytecode
 *       contains zero direct {@code INVOKEINTERFACE} on the
 *       no-Materialized overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Materialized Materialized}
 * naming the state store — e.g. {@code grouped.aggregate(
 * QuantileSketch::new, aggregator, Materialized.as("p99-sketch-
 * store"))}. The explicit name pins BOTH the state store name
 * AND the changelog topic name ({@code app-id-p99-sketch-store-
 * changelog}). For full naming coverage (state store + processor
 * node + changelog), use the 4-arg overload {@code
 * aggregate(Initializer, Aggregator, Named, Materialized)} on
 * KGroupedStream / TimeWindowedKStream, the 5-arg {@code
 * aggregate(Initializer, Aggregator, Aggregator, Named,
 * Materialized)} on KGroupedTable, or the 5-arg {@code
 * aggregate(Initializer, Aggregator, Merger, Named, Materialized)}
 * on SessionWindowedKStream.
 */
public final class StreamsAggregateNoMaterializedRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.GROUPED_KSTREAM_OWNERS;
    private static final String METHOD_NAME = "aggregate";
    private static final String MATERIALIZED_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Materialized;";

    private final Severity severity;

    public StreamsAggregateNoMaterializedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_AGGREGATE_NO_MATERIALIZED;
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
                RuleId.STREAMS_AGGREGATE_NO_MATERIALIZED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KGroupedStream / KGroupedTable / TimeWindowed"
                        + "KStream / SessionWindowedKStream . "
                        + "aggregate — a no-Materialized overload is "
                        + "reached here (the canonical "
                        + "aggregate(Initializer, Aggregator) form, "
                        + "or KGroupedTable's adder-subtractor "
                        + "aggregate(Initializer, Aggregator, "
                        + "Aggregator) form, or SessionWindowed"
                        + "KStream's aggregate(Initializer, "
                        + "Aggregator, Merger) form — none include "
                        + "Materialized) — either as a direct "
                        + "INVOKEINTERFACE on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `grouped::aggregate` bound to a "
                        + "custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). "
                        + "Of the three aggregations (count, reduce, "
                        + "aggregate), aggregate carries the LARGEST "
                        + "per-key state: a custom user-defined "
                        + "accumulator type VR which can be an "
                        + "arbitrarily-complex record (typically a "
                        + "frame-windowed snapshot, a running "
                        + "quantile sketch, a cardinality estimator, "
                        + "a maintained join-result row, etc.). When "
                        + "no Materialized is passed, the state store "
                        + "AND its companion changelog topic are "
                        + "auto-named from the topology graph index "
                        + "(e.g. KSTREAM-AGGREGATE-STATE-STORE-"
                        + "0000000007 and app-id-KSTREAM-AGGREGATE-"
                        + "STATE-STORE-0000000007-changelog). Any "
                        + "topology edit upstream renames the state "
                        + "store and the changelog; the new state "
                        + "store starts empty (re-initialized from "
                        + "the Initializer's seed value, not from "
                        + "the changelog because the changelog has "
                        + "also been renamed); the OLD changelog "
                        + "topic is orphaned on broker disk with "
                        + "full retention. Because aggregate's state "
                        + "can be substantially larger than count's "
                        + "(custom VR record vs. Long), the orphaned "
                        + "changelog disk footprint is "
                        + "proportionally larger too — typically "
                        + "50x to 500x the volume of an orphaned "
                        + "count changelog at the same input "
                        + "throughput. Concrete failure modes: (1) "
                        + "running quantile sketch is lost; SLO "
                        + "dashboards lie — team has requests."
                        + "groupByKey().aggregate(QuantileSketch::"
                        + "new, (k, v, acc) -> acc.add(v.latency())) "
                        + "backing the SLO dashboard; after one "
                        + "upstream topology edit the graph index "
                        + "shifts, Streams creates a NEW state store "
                        + "and a NEW changelog topic (both empty), "
                        + "the sketches reset to the Initializer's "
                        + "seed (an empty sketch), the p99 latency "
                        + "reads near-zero for the warm-up window "
                        + "because an empty sketch is small until "
                        + "populated, the SLO dashboard goes green, "
                        + "the SRE team that was considering a "
                        + "paging-rule loosen looks at the dashboard "
                        + "and proceeds, real latency was actually "
                        + "elevated during this window but masked "
                        + "by the seed-reset; (2) custom-VR "
                        + "changelog orphans carry 50x-500x the "
                        + "disk footprint of a count orphan — for "
                        + "an aggregate maintaining a 4 KB sketch "
                        + "per key against 1k keys and 1k updates/"
                        + "sec the changelog runs at ~4 MB/sec or "
                        + "72 GB per active day, vs. a count "
                        + "changelog at 1k updates/sec carrying 8-"
                        + "byte longs (~8 KB/sec, 148 MB per active "
                        + "day); an org with 20 aggregate(no-"
                        + "Materialized) call sites and 4 topology "
                        + "edits per site per year accumulates ~80 "
                        + "orphan custom-VR changelog topics per "
                        + "year, each carrying full retention; that "
                        + "is the canonical \"Kafka cluster is "
                        + "running out of disk and nobody knows "
                        + "why\" incident; (3) KGroupedTable."
                        + "aggregate(Initializer, Aggregator, "
                        + "Aggregator) (adder + subtractor) carries "
                        + "the same hazard — KTable aggregations "
                        + "track NET state per key, every input "
                        + "record runs through both the adder AND "
                        + "the subtractor of the prior value, "
                        + "custom-VR running state on the "
                        + "KGroupedTable path (typically a "
                        + "maintained join-row or a derived "
                        + "denormalized record) is even more "
                        + "expensive to lose than the KGroupedStream "
                        + "case because rebuilding it requires "
                        + "replaying BOTH the upstream KTable's "
                        + "changelog and re-running the subtract-"
                        + "then-add transition for every key — a 2x "
                        + "replay cost at minimum; (4) "
                        + "stateful-restore time blows up after "
                        + "every edit — even if the team eventually "
                        + "realises the sketches reset and recovers "
                        + "by re-deriving them from a source topic "
                        + "replay, the recovery cost for a custom-"
                        + "VR aggregator is proportional to the "
                        + "input volume * the topology's retention "
                        + "window — a 7-day SLO dashboard backed by "
                        + "a p99 sketch may need a 7-day input "
                        + "replay to reach steady state, the "
                        + "Streams instances handle 2x throughput "
                        + "(input + rebuild) for the full replay "
                        + "window; (5) INVOKEDYNAMIC method-"
                        + "reference captures bypass naive "
                        + "MethodInsnNode-only lint — an aggregate "
                        + "factory built as a custom 2-arg SAM "
                        + "AggFactory<K, V, VR> bound to grouped::"
                        + "aggregate compiles to INVOKEDYNAMIC "
                        + "whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KGroupedStream.aggregate(Initializer, "
                        + "Aggregator)KTable; the user-class "
                        + "bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Materialized "
                        + "overload, only the indy site. Migration: "
                        + "pass an explicit Materialized naming the "
                        + "state store — `grouped.aggregate("
                        + "QuantileSketch::new, aggregator, "
                        + "Materialized.as(\"p99-sketch-store\"))`. "
                        + "The explicit name pins BOTH the state "
                        + "store name AND the changelog topic name "
                        + "(app-id-p99-sketch-store-changelog). For "
                        + "full naming coverage (state store + "
                        + "processor node + changelog), use the "
                        + "4-arg aggregate(Initializer, Aggregator, "
                        + "Named, Materialized) overload on "
                        + "KGroupedStream / TimeWindowedKStream, "
                        + "the 5-arg aggregate(Initializer, "
                        + "Aggregator, Aggregator, Named, "
                        + "Materialized) on KGroupedTable, or the "
                        + "5-arg aggregate(Initializer, Aggregator, "
                        + "Merger, Named, Materialized) on "
                        + "SessionWindowedKStream. The aggregate "
                        + "overloads containing Materialized are "
                        + "never flagged.");
    }
}
