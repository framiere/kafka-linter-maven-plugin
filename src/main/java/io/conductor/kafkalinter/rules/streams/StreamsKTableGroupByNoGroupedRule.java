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
 * Fires for every reach of the 1-arg
 * {@link org.apache.kafka.streams.kstream.KTable#groupBy(
 * org.apache.kafka.streams.kstream.KeyValueMapper)} overload — the
 * one whose descriptor is {@code
 * (Lorg/apache/kafka/streams/kstream/KeyValueMapper;)Lorg/apache/
 * kafka/streams/kstream/KGroupedTable;} — which produces a
 * {@link org.apache.kafka.streams.kstream.KGroupedTable} configured
 * with auto-generated repartition-topic, internal-store, and
 * changelog-topic names derived from the topology graph index.
 * Catches both direct {@code INVOKEINTERFACE} calls (from a regular
 * {@code someKTable.groupBy(mapper)} invocation) and indirect
 * {@code INVOKEDYNAMIC} method-reference captures (e.g. {@code
 * someKTable::groupBy} bound to a {@link java.util.function.Function
 * Function&lt;KeyValueMapper&lt;K, V, KeyValue&lt;K1, V1&gt;&gt;,
 * KGroupedTable&lt;K1, V1&gt;&gt;} or to a custom 1-arg SAM whose
 * erased implMethod descriptor matches — the indy bsm-args contain a
 * {@code REF_invokeInterface} Handle whose owner is {@code
 * org/apache/kafka/streams/kstream/KTable}, name is {@code groupBy},
 * descriptor matches exactly).
 *
 * <h2>Why the 1-arg overload is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KTable#groupBy(
 * org.apache.kafka.streams.kstream.KeyValueMapper)} is the
 * 1-argument overload — no {@link
 * org.apache.kafka.streams.kstream.Grouped Grouped} argument. The
 * Streams runtime must name three artifacts behind the scenes to
 * make the regrouped {@link org.apache.kafka.streams.kstream.KGroupedTable}
 * work:
 *
 * <ul>
 *   <li><b>The repartition topic.</b> A regrouping
 *       {@code KTable.groupBy} changes the partitioning key, so
 *       Streams must materialize an intermediate Kafka topic to
 *       redistribute records by the new key. Without {@link
 *       org.apache.kafka.streams.kstream.Grouped#as(String)} the
 *       topic name is derived from the application id and a
 *       sequence-numbered position in the topology graph
 *       (e.g. {@code <app-id>-KTABLE-AGGREGATE-STATE-STORE-0000000017-repartition}).
 *       Insert one upstream {@code mapValues}, the sequence number
 *       shifts, the topic is renamed at next deployment.</li>
 *   <li><b>The downstream aggregation state store.</b> Any {@code
 *       aggregate} / {@code count} / {@code reduce} on the
 *       resulting {@link org.apache.kafka.streams.kstream.KGroupedTable}
 *       creates a state store; its name and changelog-topic name
 *       follow the same auto-generated scheme.</li>
 *   <li><b>The changelog topic backing that store.</b> Same naming
 *       sensitivity. A topology edit silently abandons the existing
 *       changelog and creates a new one.</li>
 * </ul>
 *
 * <h2>Concrete failure modes (carried verbatim into the violation
 * message)</h2>
 *
 * <ul>
 *   <li><b>Aggregation restarts from offset 0 after a topology
 *       edit.</b> Production app has been running for 18 months;
 *       repartition topic {@code app-KTABLE-AGGREGATE-STATE-STORE-
 *       0000000017-repartition} contains every record that the
 *       upstream regrouping ever emitted; changelog topic for the
 *       downstream aggregation state store contains the
 *       materialized state. Team ships a hotfix that adds one
 *       upstream {@code mapValues} above the {@code groupBy}; the
 *       sequence number shifts to 0000000019; on deploy the
 *       application looks for {@code ...0000000019-repartition} —
 *       which doesn't exist — and creates it from scratch; same for
 *       the aggregation state store and its changelog. The
 *       aggregated counts restart from zero; downstream consumers
 *       see the metric drop to 0 then climb back up over the next
 *       N hours; alerts page off-hours.</li>
 *   <li><b>Cross-version state migration impossible.</b> When the
 *       application id is rotated for a version bump, the auto-
 *       generated names also rotate; there is no way to migrate
 *       the previous changelog to the new application because the
 *       new application doesn't know what name to look for.</li>
 *   <li><b>Repartition topic naming collisions and orphan topics.</b>
 *       Two independent topologies with similar shapes can produce
 *       repartition topics that look identical at deploy time
 *       (same auto-generated suffix) but with different semantics —
 *       they should not share a topic. Without {@code Grouped.as}
 *       there is no semantic name to enforce uniqueness; the team
 *       finds out by stack trace in production. Conversely, old
 *       repartition topics become orphans when the topology shifts;
 *       Kafka does not clean them up automatically and they grow
 *       unbounded.</li>
 *   <li><b>{@code INVOKEDYNAMIC} {@code KTable::groupBy} captures
 *       bypass naive {@code MethodInsnNode}-only lint.</b> A
 *       factory built as {@code Function<KeyValueMapper<K, V,
 *       KeyValue<K1, V1>>, KGroupedTable<K1, V1>> factory =
 *       someKTable::groupBy} compiles to {@code INVOKEDYNAMIC}
 *       whose bsm-args contain a {@code REF_invokeInterface} Handle
 *       pointing at {@code KTable.groupBy(KeyValueMapper)
 *       KGroupedTable}. The user-class bytecode contains zero
 *       direct {@code INVOKEINTERFACE} on the 1-arg overload — only
 *       the indy site. A rule that walks only {@code MethodInsnNode}
 *       misses every such site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — two {@code groupBy} overloads,
 * only one is the hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KTable} declares two
 * {@code groupBy} overloads:
 *
 * <ul>
 *   <li>Unsafe (auto-named): {@code (Lorg/apache/kafka/streams/
 *       kstream/KeyValueMapper;)Lorg/apache/kafka/streams/kstream/
 *       KGroupedTable;} — groupBy(mapper)</li>
 *   <li>Safe (explicit Grouped): {@code (Lorg/apache/kafka/streams/
 *       kstream/KeyValueMapper;Lorg/apache/kafka/streams/kstream/
 *       Grouped;)Lorg/apache/kafka/streams/kstream/KGroupedTable;} —
 *       groupBy(mapper, Grouped)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the 1-arg overload descriptor.
 * Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Grouped Grouped} naming the
 * regrouping — e.g. {@code groupBy((k, v) -> KeyValue.pair(...),
 * Grouped.as("orders-by-customer"))}. The chosen name must be
 * stable across topology edits because it controls the repartition
 * topic, the downstream store, and its changelog.
 */
public final class StreamsKTableGroupByNoGroupedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KTABLE);
    private static final String METHOD_NAME = "groupBy";
    private static final String UNSAFE_DESC =
            "(Lorg/apache/kafka/streams/kstream/KeyValueMapper;)"
                    + "Lorg/apache/kafka/streams/kstream/KGroupedTable;";

    private final Severity severity;

    public StreamsKTableGroupByNoGroupedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_KTABLE_GROUP_BY_NO_GROUPED;
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
                RuleId.STREAMS_KTABLE_GROUP_BY_NO_GROUPED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KTable.groupBy(KeyValueMapper) — the 1-arg overload "
                        + "with no Grouped is reached here — either as a "
                        + "direct INVOKEINTERFACE on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`someKTable::groupBy` bound to "
                        + "Function<KeyValueMapper<K, V, KeyValue<K1, V1>>, "
                        + "KGroupedTable<K1, V1>> or a custom 1-arg SAM). "
                        + "Without an explicit Grouped argument, Streams "
                        + "derives the names of three artifacts from the "
                        + "topology graph index: (a) the repartition "
                        + "topic that redistributes records by the new "
                        + "key (a KTable.groupBy that changes the key "
                        + "REQUIRES repartitioning); (b) the state store "
                        + "of any downstream aggregate/count/reduce on "
                        + "the resulting KGroupedTable; (c) the changelog "
                        + "topic backing that store. The auto-generated "
                        + "names follow the pattern `<app-id>-KTABLE-"
                        + "AGGREGATE-STATE-STORE-<N>-repartition` where "
                        + "N is a sequence number assigned by the "
                        + "topology builder — insert one upstream "
                        + "mapValues, the sequence shifts, every "
                        + "auto-generated name shifts with it. Concrete "
                        + "failure modes: (1) aggregation restarts from "
                        + "offset 0 after a topology edit — production "
                        + "app has been running for 18 months; team ships "
                        + "a hotfix that adds one upstream mapValues "
                        + "above the groupBy; the sequence number shifts; "
                        + "on deploy the application looks for "
                        + "`...<new-N>-repartition` (doesn't exist) and "
                        + "creates it from scratch; same for the "
                        + "downstream store and its changelog; aggregated "
                        + "counts restart from zero; metric drops to 0 "
                        + "then climbs back up over hours; alerts page "
                        + "off-hours; (2) cross-version state migration "
                        + "impossible — when application id is rotated "
                        + "for a version bump, auto-generated names also "
                        + "rotate; no way to migrate the previous "
                        + "changelog because the new application doesn't "
                        + "know what name to look for; (3) repartition "
                        + "topic naming collisions and orphan topics — "
                        + "two independent topologies with similar shapes "
                        + "can produce repartition topics that look "
                        + "identical at deploy time but have different "
                        + "semantics; without Grouped.as there is no "
                        + "semantic name to enforce uniqueness; "
                        + "conversely, old repartition topics become "
                        + "orphans when topology shifts (Kafka doesn't "
                        + "clean them up automatically); they grow "
                        + "unbounded; (4) INVOKEDYNAMIC `KTable::groupBy` "
                        + "captures bypass naive MethodInsnNode-only "
                        + "lint — `Function<KeyValueMapper, KGroupedTable> "
                        + "factory = someKTable::groupBy` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KTable.groupBy(KeyValueMapper)KGroupedTable; "
                        + "the user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the 1-arg overload, only "
                        + "the indy site. Migration: pass an explicit "
                        + "Grouped naming the regrouping — `groupBy((k, "
                        + "v) -> KeyValue.pair(...), Grouped.as(\"orders-"
                        + "by-customer\"))`. The chosen name must be "
                        + "stable across topology edits because it "
                        + "controls the repartition topic, the downstream "
                        + "state store, and its changelog. The 2-arg "
                        + "overload (KeyValueMapper, Grouped) is never "
                        + "flagged by this rule.");
    }
}
