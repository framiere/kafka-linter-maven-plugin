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
 * Fires for every reach of the single-argument overload of
 * {@link org.apache.kafka.streams.kstream.KStream#groupBy
 * KStream.groupBy(KeyValueMapper)} — i.e. the rekey-and-group
 * operator invoked with no {@link
 * org.apache.kafka.streams.kstream.Grouped Grouped} argument.
 *
 * <p>The unsafe descriptor is exactly {@code
 * (Lorg/apache/kafka/streams/kstream/KeyValueMapper;)Lorg/apache/
 * kafka/streams/kstream/KGroupedStream;} on {@link
 * org.apache.kafka.streams.kstream.KStream KStream}. The safe
 * sibling {@code groupBy(KeyValueMapper, Grouped)} is never
 * flagged. {@code groupByKey} (no-arg / 1-arg Grouped overloads)
 * is a separate operator — covered by {@code
 * STREAMS_GROUP_BY_KEY_NO_GROUPED}, not by this rule.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code stream::groupBy} bound to {@link
 * java.util.function.Function Function&lt;KeyValueMapper,
 * KGroupedStream&gt;} or to a custom 1-arg SAM whose erased
 * implMethod descriptor matches exactly {@code (KeyValueMapper)
 * KGroupedStream}).
 *
 * <h2>Why no-Grouped {@code groupBy(KeyValueMapper)} is THE
 * triple-artifact unnamed-node hazard</h2>
 *
 * <p>{@code KStream.groupBy(KeyValueMapper)} is the rekey-and-
 * group operator: it computes a new key from each record (via
 * the {@link org.apache.kafka.streams.kstream.KeyValueMapper
 * KeyValueMapper}) and then prepares the stream for a stateful
 * aggregation (count / reduce / aggregate / windowedBy). Because
 * the key changes, Streams MUST insert an auto-repartition step
 * between {@code groupBy} and the downstream aggregation —
 * partition assignment for the new key is computed from the
 * derived key's partitioner, not the upstream partition.
 *
 * <p>When no explicit {@link
 * org.apache.kafka.streams.kstream.Grouped Grouped} is passed,
 * THREE distinct graph-index-derived artifacts are spawned at
 * the {@code groupBy} node:
 *
 * <ol>
 *   <li><b>The auto-repartition topic.</b> Named after the
 *       graph-index — e.g. {@code app-id-KSTREAM-AGGREGATE-
 *       STATE-STORE-0000000005-repartition}. Carries the full
 *       upstream throughput. Any topology edit upstream renames
 *       it; the OLD repartition topic is orphaned on broker
 *       disk with full retention; the new one starts empty.</li>
 *   <li><b>The downstream aggregation's state store.</b> Named
 *       {@code KSTREAM-AGGREGATE-STATE-STORE-0000000005} (or
 *       {@code KSTREAM-REDUCE-STATE-STORE-0000000007}, etc.).
 *       Same graph-index-derived name; topology edits rename it
 *       and the aggregation starts from the initializer's seed
 *       (count: 0; reduce: first record; aggregate: initializer
 *       return value).</li>
 *   <li><b>The state-store changelog topic.</b> Named {@code
 *       app-id-KSTREAM-AGGREGATE-STATE-STORE-0000000005-
 *       changelog}. Same graph-index-derived name; topology
 *       edits rename it AND orphan the old one (with full
 *       retention).</li>
 * </ol>
 *
 * <p>This is the same triple-artifact hazard pattern as a
 * KStream-KStream join with no StreamJoined ({@code
 * STREAMS_STREAM_JOIN_NO_NAMED}), but with one repartition
 * topic instead of two and one state store instead of two — so
 * three artifacts instead of five. It is THE most common
 * unnamed-node hazard in real Kafka Streams code because
 * groupBy-then-aggregate is the canonical Streams DSL pattern.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Aggregation silently restarts from zero on topology
 *       edit.</b> Team has {@code clicks.groupBy((k, v) ->
 *       v.userId()).count()} — the daily-active-user counter.
 *       After one upstream topology edit (e.g. inserting a
 *       {@code filter} step between the source and the
 *       groupBy), the graph index shifts; Streams creates a
 *       NEW state store and a NEW changelog topic (both empty);
 *       the counter restarts from zero on the next deploy; the
 *       DAU dashboard shows a step-discontinuity that takes
 *       hours of investigation to attribute (the topology edit
 *       was unrelated to the DAU counter, so engineering does
 *       not initially correlate them).</li>
 *   <li><b>Triple-artifact orphan accumulation per topology
 *       edit.</b> Each topology edit upstream of an unnamed
 *       groupBy leaves behind: 1 old auto-repartition topic + 1
 *       old state-store changelog = 2 broker-side artifacts
 *       (plus the per-instance state-store local disk). For an
 *       org with 80 groupBy(KeyValueMapper) call sites and an
 *       average of 6 topology edits per call site per year,
 *       that's 80 * 6 * 2 = 960 orphan broker-side artifacts
 *       per year, each carrying retention-bound disk
 *       footprint.</li>
 *   <li><b>Grafana aggregation panels rebrand on every edit.</b>
 *       Panels filter on {@code topic="app-id-KSTREAM-
 *       AGGREGATE-STATE-STORE-0000000005-repartition"} for
 *       repartition-write-rate, on {@code topic="app-id-
 *       KSTREAM-AGGREGATE-STATE-STORE-0000000005-changelog"}
 *       for changelog-write-rate, and on {@code processor-node-
 *       id="KSTREAM-AGGREGATE-0000000007"} for aggregate-
 *       process-rate. ALL THREE rebrand on any topology edit
 *       upstream; the aggregation's entire observability
 *       surface disappears from existing dashboards in one
 *       deploy.</li>
 *   <li><b>topology.describe() runbook drift.</b> SRE runbooks
 *       reference the aggregation pipeline by its auto-generated
 *       node names — {@code "if KSTREAM-AGGREGATE-0000000007
 *       process-rate drops below 1k/s, check the upstream
 *       groupBy"}. After any topology edit the named node no
 *       longer exists; the runbook step silently no-ops; the
 *       operator believes they are monitoring the user-by-id
 *       aggregation but the wrong node (or no node at all) was
 *       targeted.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       group-by factory built as {@code Function&lt;
 *       KeyValueMapper&lt;String, V, String&gt;, KGroupedStream
 *       &lt;String, V&gt;&gt; groupByFactory = stream::groupBy}
 *       compiles to an {@code INVOKEDYNAMIC} site whose
 *       bsm-args contain a {@code REF_invokeInterface} Handle
 *       pointing at {@code KStream.groupBy(KeyValueMapper)
 *       KGroupedStream}. The user-class bytecode contains zero
 *       direct {@code INVOKEINTERFACE} on the no-Grouped
 *       overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Grouped Grouped} with a
 * stable name — e.g. {@code stream.groupBy((k, v) ->
 * v.userId(), Grouped.as("clicks-by-user"))}. The explicit name
 * pins the auto-repartition topic name ({@code app-id-clicks-
 * by-user-repartition}), the downstream state-store name (if
 * the immediately-following operator does not pass its own
 * Materialized, the state store name is also derived from the
 * groupBy's Grouped name), and the changelog topic name —
 * stabilizing them across topology edits. For complete naming
 * coverage, pair {@code Grouped.as(...)} with {@code
 * Materialized.as(...)} on the following aggregation operator.
 */
public final class StreamsGroupByNoGroupedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "groupBy";
    private static final String UNSAFE_DESC =
            "(Lorg/apache/kafka/streams/kstream/KeyValueMapper;)Lorg/apache/kafka/streams/kstream/KGroupedStream;";

    private final Severity severity;

    public StreamsGroupByNoGroupedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_GROUP_BY_NO_GROUPED;
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
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, UNSAFE_DESC);
                    if (h != null) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_GROUP_BY_NO_GROUPED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.groupBy(KeyValueMapper) — the no-Grouped "
                        + "single-argument overload is reached here — "
                        + "either as a direct INVOKEINTERFACE on the "
                        + "method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `stream::groupBy` "
                        + "bound to Function<KeyValueMapper, "
                        + "KGroupedStream> or to a custom 1-arg SAM "
                        + "whose erased implMethod descriptor matches "
                        + "(KeyValueMapper)KGroupedStream exactly). "
                        + "groupBy is the rekey-and-group operator — "
                        + "it computes a new key from each record and "
                        + "prepares the stream for a stateful "
                        + "aggregation; because the key changes, "
                        + "Streams MUST insert an auto-repartition "
                        + "step between groupBy and the downstream "
                        + "aggregation. When no explicit Grouped is "
                        + "passed, THREE distinct graph-index-derived "
                        + "artifacts are spawned: (a) the auto-"
                        + "repartition topic (e.g. app-id-KSTREAM-"
                        + "AGGREGATE-STATE-STORE-0000000005-"
                        + "repartition), (b) the downstream "
                        + "aggregation's state store (e.g. KSTREAM-"
                        + "AGGREGATE-STATE-STORE-0000000005), and (c) "
                        + "the state-store changelog topic (e.g. app-"
                        + "id-KSTREAM-AGGREGATE-STATE-STORE-"
                        + "0000000005-changelog). All three are "
                        + "derived from the same shifting graph "
                        + "index — a single topology edit upstream "
                        + "renames ALL THREE and orphans the old "
                        + "versions on broker disk with full "
                        + "retention. This is THE most common "
                        + "unnamed-node hazard in real Streams code "
                        + "because groupBy-then-aggregate is the "
                        + "canonical Streams DSL pattern. Concrete "
                        + "failure modes: (1) aggregation silently "
                        + "restarts from zero on topology edit — team "
                        + "has clicks.groupBy((k, v) -> v.userId())"
                        + ".count() (the daily-active-user counter); "
                        + "after one upstream topology edit (e.g. "
                        + "inserting a filter step between the source "
                        + "and the groupBy) the graph index shifts, "
                        + "Streams creates a NEW state store and a "
                        + "NEW changelog topic (both empty), the "
                        + "counter restarts from zero on the next "
                        + "deploy, the DAU dashboard shows a step-"
                        + "discontinuity that takes hours of "
                        + "investigation to attribute because the "
                        + "topology edit was unrelated to the DAU "
                        + "counter so engineering does not initially "
                        + "correlate them; (2) triple-artifact orphan "
                        + "accumulation per topology edit — each "
                        + "topology edit upstream of an unnamed "
                        + "groupBy leaves behind 1 old auto-"
                        + "repartition topic + 1 old state-store "
                        + "changelog = 2 broker-side artifacts (plus "
                        + "the per-instance state-store local disk); "
                        + "for an org with 80 groupBy(KeyValueMapper) "
                        + "call sites and an average of 6 topology "
                        + "edits per call site per year that's 80 * "
                        + "6 * 2 = 960 orphan broker-side artifacts "
                        + "per year, each carrying retention-bound "
                        + "disk footprint; (3) Grafana aggregation "
                        + "panels rebrand on every edit — panels "
                        + "filter on topic=\"app-id-KSTREAM-AGGREGATE-"
                        + "STATE-STORE-0000000005-repartition\" for "
                        + "repartition-write-rate, on topic=\"app-id-"
                        + "KSTREAM-AGGREGATE-STATE-STORE-0000000005-"
                        + "changelog\" for changelog-write-rate, and "
                        + "on processor-node-id=\"KSTREAM-AGGREGATE-"
                        + "0000000007\" for aggregate-process-rate; "
                        + "ALL THREE rebrand on any topology edit "
                        + "upstream and the aggregation's entire "
                        + "observability surface disappears from "
                        + "existing dashboards in one deploy; (4) "
                        + "topology.describe() runbook drift — SRE "
                        + "runbooks reference the aggregation "
                        + "pipeline by its auto-generated node names "
                        + "(\"if KSTREAM-AGGREGATE-0000000007 "
                        + "process-rate drops below 1k/s, check the "
                        + "upstream groupBy\"); after any topology "
                        + "edit the named node no longer exists, the "
                        + "runbook step silently no-ops, the operator "
                        + "believes they are monitoring the user-by-"
                        + "id aggregation but the wrong node (or no "
                        + "node at all) was targeted; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "`Function<KeyValueMapper<String, V, "
                        + "String>, KGroupedStream<String, V>> "
                        + "groupByFactory = stream::groupBy` compiles "
                        + "to INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KStream.groupBy(KeyValueMapper)"
                        + "KGroupedStream; the user-class bytecode "
                        + "contains zero direct INVOKEINTERFACE on "
                        + "the no-Grouped overload, only the indy "
                        + "site. Migration: pass an explicit Grouped "
                        + "with a stable name — `stream.groupBy((k, "
                        + "v) -> v.userId(), Grouped.as(\"clicks-by-"
                        + "user\"))`. The explicit name pins the "
                        + "auto-repartition topic name (app-id-"
                        + "clicks-by-user-repartition), the "
                        + "downstream state-store name (if the "
                        + "immediately-following operator does not "
                        + "pass its own Materialized, the state "
                        + "store name is also derived from the "
                        + "groupBy's Grouped name), and the changelog "
                        + "topic name. For complete naming coverage, "
                        + "pair Grouped.as(...) with Materialized.as"
                        + "(...) on the following aggregation "
                        + "operator. The groupBy(KeyValueMapper, "
                        + "Grouped) overload is never flagged. "
                        + "groupByKey is a separate operator — "
                        + "covered by STREAMS_GROUP_BY_KEY_NO_GROUPED, "
                        + "not by this rule.");
    }
}
