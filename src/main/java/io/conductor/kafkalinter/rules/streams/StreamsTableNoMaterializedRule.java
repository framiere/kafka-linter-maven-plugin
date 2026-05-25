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
 * {@link org.apache.kafka.streams.StreamsBuilder#table(String)} or
 * {@link org.apache.kafka.streams.StreamsBuilder#table(String,
 * org.apache.kafka.streams.kstream.Consumed)} overload — the two
 * overloads that do NOT take a
 * {@link org.apache.kafka.streams.kstream.Materialized} argument and
 * therefore leave the local KeyValueStore and its changelog topic
 * auto-named from the topology graph index. Catches both direct
 * {@code INVOKEVIRTUAL} calls (from a regular {@code
 * builder.table("topic")} invocation) and indirect {@code
 * INVOKEDYNAMIC} method-reference captures (e.g. {@code
 * builder::table} bound to a {@link java.util.function.Function
 * Function&lt;String, KTable&lt;K, V&gt;&gt;} or to a
 * {@link java.util.function.BiFunction BiFunction&lt;String,
 * Consumed, KTable&lt;K, V&gt;&gt;} or to a custom SAM whose erased
 * implMethod descriptor matches one of the unsafe overloads — the
 * indy bsm-args contain a {@code REF_invokeVirtual} Handle whose
 * owner is {@code org/apache/kafka/streams/StreamsBuilder}, name is
 * {@code table}, descriptor matches exactly one of the two unsafe
 * overload descriptors).
 *
 * <h2>Why the no-Materialized overloads are a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.StreamsBuilder#table(String)}
 * and {@link org.apache.kafka.streams.StreamsBuilder#table(String,
 * org.apache.kafka.streams.kstream.Consumed)} materialize a local
 * {@link org.apache.kafka.streams.state.KeyValueStore KeyValueStore}
 * behind the {@link org.apache.kafka.streams.kstream.KTable KTable}
 * and create a Kafka changelog topic to durably back it. Without an
 * explicit {@link org.apache.kafka.streams.kstream.Materialized}
 * argument, two artifacts get auto-generated names derived from the
 * topology graph index:
 *
 * <ul>
 *   <li><b>The local state store.</b> Without {@link
 *       org.apache.kafka.streams.kstream.Materialized#as(String)}
 *       the store name is derived from the topology graph
 *       (e.g. {@code KTABLE-SOURCE-STATE-STORE-0000000003}). Insert
 *       one upstream {@code stream/map/filter} before this {@code
 *       table} call, the graph-index sequence number shifts, the
 *       store gets renamed at next deployment.</li>
 *   <li><b>The changelog topic backing that store.</b> The
 *       changelog topic name follows {@code <app-id>-<store-name>-
 *       changelog}; if the store name shifts, the changelog topic
 *       name shifts with it. Streams looks for the new changelog
 *       at deploy time, doesn't find it, and creates an empty one.
 *       The local store starts empty.</li>
 * </ul>
 *
 * <h2>Concrete failure modes (carried verbatim into the violation
 * message)</h2>
 *
 * <ul>
 *   <li><b>KTable lookup returns null after a topology edit.</b>
 *       Production app has a {@code users} KTable backing a
 *       streams-table enrichment join; the changelog has 50M
 *       records and 18 months of history. Team ships a hotfix that
 *       adds one upstream {@code mapValues} above the {@code
 *       builder.table("users")} call (e.g. masking PII for a
 *       compliance requirement); the graph-index shifts; on
 *       deploy, Streams creates a new changelog topic at the new
 *       auto-name and a new empty store; the join produces null on
 *       the right side for every record while the new store
 *       slowly fills; downstream consumers see enrichment
 *       success rate drop to 0% then climb back over hours;
 *       alerts page off-hours. The old changelog topic becomes an
 *       orphan with 50M unreferenced records that Kafka doesn't
 *       clean up.</li>
 *   <li><b>Cross-version state migration impossible.</b> When the
 *       application id is rotated for a version bump or for a
 *       blue-green deployment, the auto-generated store and
 *       changelog names also rotate; there is no way to migrate
 *       the previous changelog to the new application because the
 *       new application doesn't know what name to look for. The
 *       team is forced into a multi-hour reprocessing window from
 *       the source topic.</li>
 *   <li><b>Interactive queries cannot find the store.</b> A
 *       monitoring dashboard or a {@code @GET /users/:id} HTTP
 *       endpoint does {@code streams.store(StoreQueryParameters.
 *       fromNameAndType("users-store", ...))} expecting a stable
 *       store name; without {@code Materialized.as("users-store")}
 *       the store is named by the topology builder and the call
 *       throws {@code InvalidStateStoreException} or returns a
 *       store object that points at the wrong data.</li>
 *   <li><b>{@code INVOKEDYNAMIC} {@code StreamsBuilder::table}
 *       captures bypass naïve {@code MethodInsnNode}-only lint.</b>
 *       A factory built as {@code Function<String, KTable<String,
 *       String>> tableFactory = builder::table} compiles to {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeVirtual} Handle pointing at {@code
 *       StreamsBuilder.table(String)KTable}. The user-class
 *       bytecode contains zero direct {@code INVOKEVIRTUAL} on the
 *       no-Materialized overloads — only the indy site. A rule
 *       that walks only {@code MethodInsnNode} misses every such
 *       site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — four {@code table} overloads, only
 * two are the hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.StreamsBuilder} declares four
 * {@code table} overloads:
 *
 * <ul>
 *   <li>Unsafe (auto-named store): {@code (Ljava/lang/String;)Lorg/
 *       apache/kafka/streams/kstream/KTable;} — table(topic)</li>
 *   <li>Unsafe (auto-named store): {@code (Ljava/lang/String;Lorg/
 *       apache/kafka/streams/kstream/Consumed;)Lorg/apache/kafka/
 *       streams/kstream/KTable;} — table(topic, Consumed)</li>
 *   <li>Safe (explicit store name via Materialized): {@code
 *       (Ljava/lang/String;Lorg/apache/kafka/streams/kstream/
 *       Materialized;)Lorg/apache/kafka/streams/kstream/KTable;} —
 *       table(topic, Materialized)</li>
 *   <li>Safe: {@code (Ljava/lang/String;Lorg/apache/kafka/streams/
 *       kstream/Consumed;Lorg/apache/kafka/streams/kstream/
 *       Materialized;)Lorg/apache/kafka/streams/kstream/KTable;} —
 *       table(topic, Consumed, Materialized)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the two unsafe overload
 * descriptors. Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Materialized} naming the local
 * store — e.g. {@code builder.table("users",
 * Materialized.<String, User, KeyValueStore<Bytes, byte[]>>as(
 * "users-store"))}. The chosen store name must be stable across
 * topology edits because it controls the changelog topic name as
 * well, and interactive queries depend on it.
 */
public final class StreamsTableNoMaterializedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.STREAMS_BUILDER);
    private static final String METHOD_NAME = "table";
    private static final String UNSAFE_DESC_TOPIC =
            "(Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KTable;";
    private static final String UNSAFE_DESC_TOPIC_CONSUMED =
            "(Ljava/lang/String;Lorg/apache/kafka/streams/kstream/Consumed;)"
                    + "Lorg/apache/kafka/streams/kstream/KTable;";

    private final Severity severity;

    public StreamsTableNoMaterializedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_TABLE_NO_MATERIALIZED;
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
                RuleId.STREAMS_TABLE_NO_MATERIALIZED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "StreamsBuilder.table(topic) / table(topic, Consumed) "
                        + "— a no-Materialized overload is reached here "
                        + "— either as a direct INVOKEVIRTUAL on the "
                        + "method or as an INVOKEDYNAMIC method-reference "
                        + "capture (e.g. `builder::table` bound to "
                        + "Function<String, KTable<K, V>> or "
                        + "BiFunction<String, Consumed, KTable<K, V>> or "
                        + "a custom SAM whose erased implMethod "
                        + "descriptor matches). Without an explicit "
                        + "Materialized argument, two artifacts get "
                        + "auto-generated names derived from the topology "
                        + "graph index: (a) the local KeyValueStore "
                        + "backing the KTable (named e.g. "
                        + "`KTABLE-SOURCE-STATE-STORE-0000000003`); (b) "
                        + "the Kafka changelog topic durably backing "
                        + "that store (named `<app-id>-<store-name>-"
                        + "changelog`). Insert one upstream "
                        + "stream/map/filter, the graph-index sequence "
                        + "shifts, the store and its changelog get "
                        + "renamed at next deployment. Concrete failure "
                        + "modes: (1) KTable lookup returns null after a "
                        + "topology edit — production app has a `users` "
                        + "KTable backing a stream-table join; changelog "
                        + "has 50M records and 18 months of history; "
                        + "team ships a hotfix that adds one upstream "
                        + "mapValues above `builder.table(\"users\")` "
                        + "(e.g. masking PII for compliance); graph-"
                        + "index shifts; on deploy Streams creates a "
                        + "new changelog topic at the new auto-name and "
                        + "a new empty store; the join produces null on "
                        + "the right side for every record while the new "
                        + "store slowly fills; enrichment success rate "
                        + "drops to 0% then climbs back over hours; "
                        + "alerts page off-hours; the old changelog "
                        + "becomes an orphan with 50M unreferenced "
                        + "records that Kafka doesn't clean up; (2) "
                        + "cross-version state migration impossible — "
                        + "when application id is rotated for a version "
                        + "bump or blue-green deployment, auto-generated "
                        + "store and changelog names rotate; no way to "
                        + "migrate the previous changelog because the "
                        + "new application doesn't know what name to "
                        + "look for; team forced into a multi-hour "
                        + "reprocessing window from the source topic; "
                        + "(3) interactive queries cannot find the "
                        + "store — a monitoring dashboard or HTTP "
                        + "endpoint does `streams.store(StoreQuery"
                        + "Parameters.fromNameAndType(\"users-store\","
                        + " ...))` expecting a stable store name; "
                        + "without Materialized.as(\"users-store\") the "
                        + "store is named by the topology builder and "
                        + "the call throws InvalidStateStoreException "
                        + "or returns a store object that points at the "
                        + "wrong data; (4) INVOKEDYNAMIC `builder::"
                        + "table` captures bypass naive MethodInsnNode-"
                        + "only lint — `Function<String, KTable<K, V>> "
                        + "tableFactory = builder::table` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeVirtual Handle pointing at "
                        + "StreamsBuilder.table(String)KTable; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEVIRTUAL on the no-Materialized "
                        + "overload, only the indy site. Migration: "
                        + "pass an explicit Materialized naming the "
                        + "local store — `builder.table(\"users\", "
                        + "Materialized.<String, User, KeyValueStore<"
                        + "Bytes, byte[]>>as(\"users-store\"))` or the "
                        + "three-arg overload `builder.table(\"users\","
                        + " Consumed.with(...), Materialized.as("
                        + "\"users-store\"))`. The chosen store name "
                        + "must be stable across topology edits because "
                        + "it controls the changelog topic name as well, "
                        + "and interactive queries depend on it. The "
                        + "two safe overloads (with Materialized) are "
                        + "never flagged by this rule.");
    }
}
