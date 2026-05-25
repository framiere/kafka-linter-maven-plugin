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
 * Fires for every reach of a
 * {@link org.apache.kafka.streams.StreamsBuilder#globalTable(String)}
 * or {@link org.apache.kafka.streams.StreamsBuilder#globalTable(
 * String, org.apache.kafka.streams.kstream.Consumed)} overload —
 * the two overloads that do NOT take a {@link
 * org.apache.kafka.streams.kstream.Materialized} argument and
 * therefore leave the global state store auto-named from the
 * topology graph index. Catches both direct {@code INVOKEVIRTUAL}
 * calls and indirect {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code builder::globalTable} bound to a
 * {@link java.util.function.Function Function&lt;String,
 * GlobalKTable&lt;K, V&gt;&gt;} or to a
 * {@link java.util.function.BiFunction BiFunction&lt;String,
 * Consumed, GlobalKTable&lt;K, V&gt;&gt;} or to a custom SAM —
 * the indy bsm-args contain a {@code REF_invokeVirtual} Handle
 * whose owner is {@code org/apache/kafka/streams/StreamsBuilder},
 * name is {@code globalTable}, descriptor matches exactly one of
 * the two unsafe overload descriptors).
 *
 * <h2>Why the no-Materialized overloads are a correctness hazard
 * (and why this is worse than the non-global case)</h2>
 *
 * <p>A {@link org.apache.kafka.streams.kstream.GlobalKTable
 * GlobalKTable} is loaded from a Kafka topic onto every Streams
 * instance in the cluster, in full, before any processing begins.
 * Unlike a regular {@link
 * org.apache.kafka.streams.kstream.KTable}, which is partitioned
 * and only the locally-assigned partitions are loaded, a
 * GlobalKTable consumes the entire topic on every instance. This
 * makes the no-Materialized hazard sharper:
 *
 * <ul>
 *   <li><b>The global state store</b> is auto-named (e.g.
 *       {@code KTABLE-SOURCE-STATE-STORE-0000000005}) and rebuilt
 *       in full from the source topic on every Streams instance
 *       at startup. Without an explicit {@link
 *       org.apache.kafka.streams.kstream.Materialized#as(String)}
 *       any topology edit shifts the sequence number and renames
 *       the store. On deploy, every Streams instance throws away
 *       the existing global store and restores from scratch.</li>
 *   <li><b>Cluster-wide cold-start cost.</b> Restoring a global
 *       store the size of {@code N} records on a cluster of {@code
 *       M} instances costs {@code N * M} reads, all of which must
 *       complete before processing can begin. A 100M-record global
 *       store on a 20-instance cluster forces a 2B-record cold
 *       start. During this window, no processing happens —
 *       stream-globalTable joins are blocked behind global-store
 *       restore.</li>
 *   <li><b>Stream-globalTable joins return null during restore.</b>
 *       Unlike KStream/KTable joins, GlobalKTable joins are
 *       synchronous and do not wait for the store to be ready —
 *       they query whatever state the store has at the time of the
 *       join, which during cold-restore is partial. A
 *       stream-globalTable join during a global-store rebuild
 *       returns null on the right side for keys that should match.
 *       The join silently produces wrong output for the entire
 *       restore window.</li>
 * </ul>
 *
 * <h2>Concrete failure modes (carried verbatim into the violation
 * message)</h2>
 *
 * <ul>
 *   <li><b>Multi-hour cold-restart and silently-wrong joins after a
 *       topology edit.</b> Production app has a {@code products}
 *       GlobalKTable backing a stream-globalTable enrichment join
 *       on incoming order events; products topic has 10M records;
 *       cluster has 12 instances; existing global stores hold the
 *       same 10M records each. Team ships a hotfix that adds one
 *       upstream {@code mapValues} above {@code
 *       builder.globalTable("products")} (e.g. dropping a deprecated
 *       field); the topology-graph sequence shifts; on deploy,
 *       every Streams instance throws away its existing 10M-record
 *       global store and restores from scratch — 120M reads
 *       cluster-wide; takes 2-3 hours; during the entire window,
 *       the order-enrichment stream-globalTable join produces null
 *       on the right side for every record; downstream business
 *       sees enrichment success rate drop to 0% then climb back
 *       over hours; alerts page off-hours.</li>
 *   <li><b>Cross-version state migration impossible.</b> When the
 *       application id is rotated for a version bump or blue-green
 *       deployment, the auto-generated global-store name rotates;
 *       there is no way to migrate the previous global state to
 *       the new application. The team is forced into a full
 *       reprocessing window for every Streams instance — for a
 *       GlobalKTable that's the entire source topic.</li>
 *   <li><b>Interactive queries on a GlobalKTable fail.</b> A
 *       monitoring dashboard or HTTP endpoint does {@code
 *       streams.store(StoreQueryParameters.fromNameAndType(
 *       "products-store", QueryableStoreTypes.<String,
 *       Product>keyValueStore()))} expecting a stable global-store
 *       name; without {@code Materialized.as("products-store")}
 *       the store is named by the topology builder and the call
 *       throws {@code InvalidStateStoreException} or returns a
 *       store object that points at the wrong data.</li>
 *   <li><b>{@code INVOKEDYNAMIC} {@code StreamsBuilder::globalTable}
 *       captures bypass naïve {@code MethodInsnNode}-only lint.</b>
 *       A factory built as {@code Function<String, GlobalKTable<
 *       String, Product>> tableFactory = builder::globalTable}
 *       compiles to {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} Handle pointing at {@code
 *       StreamsBuilder.globalTable(String)GlobalKTable}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEVIRTUAL} on the no-Materialized overloads — only the
 *       indy site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — four {@code globalTable}
 * overloads, only two are the hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.StreamsBuilder} declares four
 * {@code globalTable} overloads:
 *
 * <ul>
 *   <li>Unsafe (auto-named store): {@code (Ljava/lang/String;)Lorg/
 *       apache/kafka/streams/kstream/GlobalKTable;} —
 *       globalTable(topic)</li>
 *   <li>Unsafe (auto-named store): {@code (Ljava/lang/String;Lorg/
 *       apache/kafka/streams/kstream/Consumed;)Lorg/apache/kafka/
 *       streams/kstream/GlobalKTable;} — globalTable(topic,
 *       Consumed)</li>
 *   <li>Safe (explicit store name via Materialized): {@code (Ljava/
 *       lang/String;Lorg/apache/kafka/streams/kstream/Materialized;
 *       )Lorg/apache/kafka/streams/kstream/GlobalKTable;} —
 *       globalTable(topic, Materialized)</li>
 *   <li>Safe: {@code (Ljava/lang/String;Lorg/apache/kafka/streams/
 *       kstream/Consumed;Lorg/apache/kafka/streams/kstream/
 *       Materialized;)Lorg/apache/kafka/streams/kstream/
 *       GlobalKTable;} — globalTable(topic, Consumed, Materialized)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the two unsafe overload
 * descriptors. Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Materialized} naming the global
 * store — e.g. {@code builder.globalTable("products",
 * Materialized.<String, Product, KeyValueStore<Bytes,
 * byte[]>>as("products-store"))}. The chosen store name must be
 * stable across topology edits because cold-restart cost is
 * cluster-wide and joins return null during restore.
 */
public final class StreamsGlobalTableNoMaterializedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.STREAMS_BUILDER);
    private static final String METHOD_NAME = "globalTable";
    private static final String UNSAFE_DESC_TOPIC =
            "(Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/GlobalKTable;";
    private static final String UNSAFE_DESC_TOPIC_CONSUMED =
            "(Ljava/lang/String;Lorg/apache/kafka/streams/kstream/Consumed;)"
                    + "Lorg/apache/kafka/streams/kstream/GlobalKTable;";

    private final Severity severity;

    public StreamsGlobalTableNoMaterializedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_GLOBAL_TABLE_NO_MATERIALIZED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && (UNSAFE_DESC_TOPIC.equals(mi.desc)
                                || UNSAFE_DESC_TOPIC_CONSUMED.equals(mi.desc))) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && (AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, UNSAFE_DESC_TOPIC) != null
                                || AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, UNSAFE_DESC_TOPIC_CONSUMED) != null)) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_GLOBAL_TABLE_NO_MATERIALIZED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "StreamsBuilder.globalTable(topic) / globalTable("
                        + "topic, Consumed) — a no-Materialized overload "
                        + "is reached here — either as a direct "
                        + "INVOKEVIRTUAL on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`builder::globalTable` bound to "
                        + "Function<String, GlobalKTable<K, V>> or "
                        + "BiFunction<String, Consumed, GlobalKTable<K, "
                        + "V>> or a custom SAM whose erased implMethod "
                        + "descriptor matches). A GlobalKTable is "
                        + "loaded from a Kafka topic onto EVERY Streams "
                        + "instance in the cluster, in full, before "
                        + "processing begins — unlike a regular "
                        + "KTable, which is partitioned and only the "
                        + "locally-assigned partitions are loaded. "
                        + "Without an explicit Materialized argument, "
                        + "the global state store is auto-named (e.g. "
                        + "`KTABLE-SOURCE-STATE-STORE-0000000005`) "
                        + "derived from the topology graph index; any "
                        + "topology edit shifts the sequence number "
                        + "and renames the store; on deploy, every "
                        + "Streams instance throws away the existing "
                        + "global store and restores from scratch. "
                        + "Concrete failure modes: (1) multi-hour "
                        + "cold-restart and silently-wrong joins after "
                        + "a topology edit — production app has a "
                        + "`products` GlobalKTable backing a stream-"
                        + "globalTable enrichment join; products topic "
                        + "has 10M records; cluster has 12 instances; "
                        + "team ships a hotfix that adds one upstream "
                        + "mapValues above `builder.globalTable("
                        + "\"products\")` (e.g. dropping a deprecated "
                        + "field); graph-index shifts; every Streams "
                        + "instance throws away its existing 10M-record "
                        + "store and restores from scratch — 120M reads "
                        + "cluster-wide, 2-3 hours; during the entire "
                        + "window the order-enrichment stream-"
                        + "globalTable join produces null on the right "
                        + "side for every record (GlobalKTable joins "
                        + "are synchronous and do NOT wait for the "
                        + "store to be ready); downstream business "
                        + "sees enrichment success rate drop to 0% "
                        + "then climb back over hours; alerts page "
                        + "off-hours; (2) cross-version state migration "
                        + "impossible — when application id is rotated "
                        + "for a version bump or blue-green deployment, "
                        + "auto-generated global-store name rotates; "
                        + "no way to migrate the previous global state "
                        + "to the new application; team forced into a "
                        + "full reprocessing window for every Streams "
                        + "instance (for a GlobalKTable that's the "
                        + "entire source topic, on every instance); "
                        + "(3) interactive queries on a GlobalKTable "
                        + "fail — a monitoring dashboard or HTTP "
                        + "endpoint does `streams.store(StoreQuery"
                        + "Parameters.fromNameAndType(\"products-store"
                        + "\", QueryableStoreTypes.<String, Product>"
                        + "keyValueStore()))` expecting a stable "
                        + "global-store name; without Materialized.as("
                        + "\"products-store\") the store is named by "
                        + "the topology builder; the call throws "
                        + "InvalidStateStoreException or returns a "
                        + "store object that points at the wrong "
                        + "data; (4) INVOKEDYNAMIC `builder::"
                        + "globalTable` captures bypass naive "
                        + "MethodInsnNode-only lint — `Function<"
                        + "String, GlobalKTable<String, Product>> "
                        + "tableFactory = builder::globalTable` "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeVirtual Handle pointing "
                        + "at StreamsBuilder.globalTable(String)"
                        + "GlobalKTable; the user-class bytecode "
                        + "contains zero direct INVOKEVIRTUAL on the "
                        + "no-Materialized overload, only the indy "
                        + "site. Migration: pass an explicit "
                        + "Materialized naming the global store — "
                        + "`builder.globalTable(\"products\", "
                        + "Materialized.<String, Product, "
                        + "KeyValueStore<Bytes, byte[]>>as("
                        + "\"products-store\"))` or the three-arg "
                        + "overload with both Consumed and "
                        + "Materialized. The chosen store name must be "
                        + "stable across topology edits because "
                        + "cold-restart cost is cluster-wide and joins "
                        + "return null during restore. The two safe "
                        + "overloads (with Materialized) are never "
                        + "flagged by this rule.");
    }
}
