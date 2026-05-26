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
 * Fires for every reach of any foreign-key {@code join} or
 * {@code leftJoin} overload on {@link
 * org.apache.kafka.streams.kstream.KTable KTable} whose
 * descriptor does NOT include a {@link
 * org.apache.kafka.streams.kstream.TableJoined TableJoined}
 * argument.
 *
 * <p>Predicate is structural — any descriptor for {@code join}
 * or {@code leftJoin} on {@link
 * org.apache.kafka.streams.kstream.KTable KTable} whose argument
 * list contains {@code java/util/function/Function} (the
 * left-key-to-foreign-key extractor) AND does NOT contain {@code
 * org/apache/kafka/streams/kstream/TableJoined} is unsafe.
 * Same-key (non-foreign-key) KTable joins use {@code (KTable,
 * ValueJoiner, ...)} without the {@code Function} extractor and
 * are NOT in scope for this rule.
 *
 * <p>The Function predicate distinguishes foreign-key joins
 * from regular same-key joins; the absence of TableJoined
 * distinguishes the unsafe overloads from the named-and-co-
 * partitioned safe overloads. So the rule fires for every
 * combination of {(join, leftJoin) × (no extra, Named-only,
 * Materialized-only, Named+Materialized)} — i.e. eight unsafe
 * FK-join descriptors — but never for the four safe overloads
 * containing TableJoined.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls and
 * {@code INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-TableJoined foreign-key {@code join()} silently
 * orphans BOTH internal subscription topics on every topology
 * edit, AND breaks every re-key partition assignment that a
 * downstream consumer relies on</h2>
 *
 * <p>A KTable-KTable foreign-key join is implemented in Kafka
 * Streams as a 4-stage dataflow: the LEFT KTable emits
 * subscription registrations keyed by the foreign key into an
 * INTERNAL "subscription registration" topic; the RIGHT KTable
 * (whose primary key matches the foreign key) joins those
 * subscriptions to its values and emits subscription responses
 * back into ANOTHER internal "subscription response" topic;
 * the response is then re-keyed back to the LEFT side's primary
 * key for emission to the joined output KTable. Both internal
 * topics are created on first start of the application from
 * the application id and processor-node graph index — they are
 * named:
 *
 * <pre>
 * {app-id}-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-{N}-topic
 * {app-id}-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-{N}-topic
 * </pre>
 *
 * <p>When no {@link
 * org.apache.kafka.streams.kstream.TableJoined TableJoined}
 * argument is passed, the {@code N} in both topic names is the
 * topology graph index of the join processor node. Any
 * upstream topology edit shifts the graph index, and on the
 * next deploy the application starts with NEW empty
 * subscription topics; the old subscription topics are
 * orphaned on the broker (still carrying all the left-side
 * registrations and right-side responses for every key that
 * was alive at the moment of cutover, plus their indefinite
 * retention because subscription topics are infinite-retention
 * by default); the new topics start empty, and the FK join
 * produces NO output until every left-side key is re-emitted.
 *
 * <p>{@link org.apache.kafka.streams.kstream.TableJoined
 * TableJoined} is the FK-join's PROCESSOR-LEVEL identity. It
 * is more than just {@link
 * org.apache.kafka.streams.kstream.Named Named} — it wraps
 * BOTH a {@link org.apache.kafka.streams.kstream.Named Named}
 * (pinning the processor-node id and both subscription topic
 * names) AND optional {@code StreamPartitioner} overrides for
 * the subscription-registration side and the subscription-
 * response side (pinning the partition assignment used when
 * subscriptions are written and read back). The processor-
 * node-name half is the load-bearing part for this rule: the
 * Named carried inside TableJoined is the one that flows into
 * the topic names. Passing {@code Named} alone on a
 * descriptor that lacks TableJoined ({@code join(KTable,
 * Function, ValueJoiner, Named)}) DOES pin the processor-node
 * id but does NOT pin the subscription topic names — those
 * descriptors are still unsafe.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>BOTH subscription topics get orphaned on every
 *       topology edit and consume disk indefinitely.</b> The
 *       subscription registration topic and the subscription
 *       response topic have INFINITE retention by default
 *       (they are state-store-like internal topics, not regular
 *       record topics). After a topology edit shifts the
 *       graph index from 5 to 6, {@code app-id-KTABLE-FK-JOIN-
 *       SUBSCRIPTION-REGISTRATION-0000000005-topic} and {@code
 *       app-id-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000005-
 *       topic} are abandoned on the broker. They are not
 *       deleted automatically; an operator has to manually
 *       identify them and delete them — but doing so requires
 *       knowing which graph index they came from, which is no
 *       longer present in any source artifact after the edit.
 *       The orphaned topics keep their partition log files
 *       open on the broker, consuming page cache and disk for
 *       no purpose.</li>
 *   <li><b>The FK join produces NO output until every left-
 *       side key is re-emitted, breaking SLOs.</b> When the
 *       topology edit creates NEW {@code SUBSCRIPTION-
 *       REGISTRATION-0000000006-topic} and {@code SUBSCRIPTION-
 *       RESPONSE-0000000006-topic}, both start empty. The FK
 *       join cannot emit a joined record for any left-side key
 *       until the LEFT KTable emits a tombstone-and-replay of
 *       that key — which happens only when the left side's
 *       upstream emits a new record for that key. For a long-
 *       tail key distribution this can take days or weeks; for
 *       keys that have already retired (their last upstream
 *       emission predates the deploy), the FK join may never
 *       re-emit them at all. Downstream queries on the joined
 *       KTable read stale-or-missing values for an unbounded
 *       window.</li>
 *   <li><b>Named-only ({@code join(KTable, Function,
 *       ValueJoiner, Named)}) is a HALF-fix that hides the
 *       problem.</b> A team that passes {@code Named.as(
 *       "fk-join")} into the 4-arg join expecting "processor
 *       node name now pinned" reads {@code topology.describe()}
 *       and sees {@code KTABLE-FK-JOIN-FK-JOIN-fk-join} in the
 *       processor-node names — appearing fully named. But the
 *       internal SUBSCRIPTION-REGISTRATION and SUBSCRIPTION-
 *       RESPONSE topic names are derived from the TableJoined
 *       Named, not the join-overload Named (which only pins the
 *       processor node, not the subscription topics). Those
 *       topic names are STILL graph-index-derived. The team
 *       believes they fixed the problem; the next topology
 *       edit silently orphans both subscription topics.</li>
 *   <li><b>Custom partitioners are unset — subscription
 *       registrations may land on the wrong partition.</b>
 *       TableJoined also carries optional StreamPartitioner
 *       overrides for both subscription topics. When the LEFT
 *       and RIGHT KTables have different upstream partitioning
 *       (e.g. left repartitioned by some custom key, right
 *       partitioned by raw key hash) and there is no
 *       TableJoined to align them, the subscription
 *       registrations may be written to a partition whose
 *       owner does not own the corresponding right-side
 *       primary key — the RIGHT KTable's task receives a
 *       registration it cannot answer, and the subscription
 *       response is never produced. The FK join silently drops
 *       the join for those left keys. The drop is invisible to
 *       the application; the joined KTable just has missing
 *       entries.</li>
 *   <li><b>Materialized-only ({@code join(..., Materialized)})
 *       fixes the joined-output store name but NOT either
 *       subscription topic.</b> Same shape as the Named-only
 *       half-fix, with the additional wrinkle that the team
 *       sees the output-store name pinned in topology.describe()
 *       and assumes the join is fully named. The 5-arg {@code
 *       join(..., Named, Materialized)} overload combines both
 *       half-fixes and still does NOT pin the subscription
 *       topics — only TableJoined does.</li>
 *   <li><b>Cross-region replication breaks on the orphans.</b>
 *       Operations that mirror the application's internal
 *       topics (MirrorMaker2, Confluent Replicator) to another
 *       cluster for DR must enumerate the topic names; the
 *       orphaned SUBSCRIPTION-REGISTRATION and SUBSCRIPTION-
 *       RESPONSE topics for the old graph index are still
 *       there, and the mirror keeps replicating them
 *       indefinitely. The replication target accumulates
 *       garbage that nobody will ever consume; on the next
 *       topology edit the same thing happens again. After a
 *       year of weekly deploys the mirror has tens of
 *       orphaned topic pairs.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> An
 *       FK join factory built as {@code BiFunction&lt;Function&lt;
 *       V, KO&gt;, ValueJoiner&lt;V, VO, VR&gt;, KTable&lt;K, VR&gt;
 *       &gt; f = leftTable::join} captures an {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle on {@code KTable.join(KTable,
 *       Function, ValueJoiner)KTable}. The user-class bytecode
 *       contains zero direct {@code INVOKEINTERFACE} on the
 *       no-TableJoined overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.TableJoined TableJoined}
 * pinning both subscription topic names — e.g. {@code
 * leftTable.join(rightTable, V::getForeignKey, JOINER,
 * TableJoined.with(Named.as("my-fk-join")))}. The TableJoined's
 * Named pins {@code app-id-my-fk-join-subscription-
 * registration-topic} and {@code app-id-my-fk-join-subscription-
 * response-topic}, and survives every topology edit. If the
 * LEFT and RIGHT KTables use custom partitioning, pass
 * StreamPartitioners into the TableJoined as well to pin the
 * subscription partition assignment.
 */
public final class StreamsForeignKeyJoinNoTableJoinedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KTABLE);
    private static final Set<String> METHOD_NAMES = Set.of("join", "leftJoin");
    private static final String FUNCTION_TOKEN = "Ljava/util/function/Function;";
    private static final String TABLE_JOINED_TOKEN =
            "Lorg/apache/kafka/streams/kstream/TableJoined;";

    private final Severity severity;

    public StreamsForeignKeyJoinNoTableJoinedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_FOREIGN_KEY_JOIN_NO_TABLE_JOINED;
    }

    private static boolean isUnsafe(String desc) {
        return desc != null
                && desc.contains(FUNCTION_TOKEN)
                && !desc.contains(TABLE_JOINED_TOKEN);
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAMES.contains(mi.name)
                        && isUnsafe(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    for (String name : METHOD_NAMES) {
                        Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, name, null);
                        if (h != null && isUnsafe(h.getDesc())) {
                            out.add(violation(ctx, mn, insn));
                            break;
                        }
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_FOREIGN_KEY_JOIN_NO_TABLE_JOINED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KTable.join / leftJoin foreign-key (KTable, "
                        + "Function<V, KO>, ValueJoiner, ...) with "
                        + "no TableJoined — reached here either as a "
                        + "direct INVOKEINTERFACE on the method or "
                        + "as an INVOKEDYNAMIC method-reference "
                        + "capture (e.g. `leftTable::join` bound to a "
                        + "BiFunction<Function, ValueJoiner, KTable> "
                        + "whose erased implMethod descriptor matches "
                        + "an unsafe FK overload). A KTable-KTable "
                        + "foreign-key join is implemented in Kafka "
                        + "Streams as a 4-stage dataflow: the LEFT "
                        + "KTable emits subscription registrations "
                        + "keyed by the foreign key into an INTERNAL "
                        + "\"subscription registration\" topic; the "
                        + "RIGHT KTable joins those subscriptions to "
                        + "its values and emits subscription "
                        + "responses back into ANOTHER internal "
                        + "\"subscription response\" topic; the "
                        + "response is then re-keyed back to the "
                        + "LEFT side's primary key for emission to "
                        + "the joined output KTable. Both internal "
                        + "topics are named app-id-KTABLE-FK-JOIN-"
                        + "SUBSCRIPTION-REGISTRATION-{N}-topic and "
                        + "app-id-KTABLE-FK-JOIN-SUBSCRIPTION-"
                        + "RESPONSE-{N}-topic, where {N} is the "
                        + "topology graph index of the join "
                        + "processor node. When no TableJoined is "
                        + "passed, any upstream topology edit shifts "
                        + "the graph index; on the next deploy the "
                        + "application starts with NEW empty "
                        + "subscription topics, the old ones are "
                        + "orphaned on the broker (still carrying "
                        + "all left-side registrations and right-"
                        + "side responses for every key alive at "
                        + "cutover, with INFINITE retention because "
                        + "subscription topics are state-store-like), "
                        + "and the FK join produces NO output until "
                        + "every left-side key is re-emitted. "
                        + "TableJoined is more than just Named — it "
                        + "wraps a Named (pinning the processor-node "
                        + "id AND both subscription topic names) AND "
                        + "optional StreamPartitioner overrides for "
                        + "the subscription registration and response "
                        + "sides. Passing Named alone on a descriptor "
                        + "that lacks TableJoined (join(KTable, "
                        + "Function, ValueJoiner, Named)) pins the "
                        + "processor-node id but does NOT pin the "
                        + "subscription topic names — that descriptor "
                        + "is still unsafe. Concrete failure modes: "
                        + "(1) BOTH subscription topics get orphaned "
                        + "on every topology edit and consume disk "
                        + "indefinitely — they have INFINITE "
                        + "retention by default (state-store-like "
                        + "internal topics, not regular record "
                        + "topics); after a topology edit shifts the "
                        + "graph index from 5 to 6, app-id-KTABLE-FK-"
                        + "JOIN-SUBSCRIPTION-REGISTRATION-0000000005-"
                        + "topic and app-id-KTABLE-FK-JOIN-"
                        + "SUBSCRIPTION-RESPONSE-0000000005-topic are "
                        + "abandoned on the broker, not deleted "
                        + "automatically; an operator has to "
                        + "manually identify them and delete them but "
                        + "doing so requires knowing which graph "
                        + "index they came from, which is no longer "
                        + "present in any source artifact after the "
                        + "edit; the orphaned topics keep their "
                        + "partition log files open on the broker, "
                        + "consuming page cache and disk for no "
                        + "purpose; (2) the FK join produces NO "
                        + "output until every left-side key is re-"
                        + "emitted, breaking SLOs — when the topology "
                        + "edit creates NEW SUBSCRIPTION-REGISTRATION-"
                        + "0000000006-topic and SUBSCRIPTION-RESPONSE-"
                        + "0000000006-topic both start empty, the FK "
                        + "join cannot emit a joined record for any "
                        + "left-side key until the LEFT KTable emits "
                        + "a tombstone-and-replay of that key (only "
                        + "when the left side's upstream emits a new "
                        + "record for that key); for a long-tail key "
                        + "distribution this can take days or weeks, "
                        + "for retired keys it may never happen; "
                        + "downstream queries on the joined KTable "
                        + "read stale-or-missing values for an "
                        + "unbounded window; (3) Named-only "
                        + "(join(KTable, Function, ValueJoiner, "
                        + "Named)) is a HALF-fix that hides the "
                        + "problem — a team that passes Named.as(\""
                        + "fk-join\") into the 4-arg join expecting "
                        + "\"processor node name now pinned\" reads "
                        + "topology.describe() and sees KTABLE-FK-"
                        + "JOIN-FK-JOIN-fk-join in the processor-"
                        + "node names appearing fully named, but the "
                        + "internal SUBSCRIPTION-REGISTRATION and "
                        + "SUBSCRIPTION-RESPONSE topic names are "
                        + "derived from the TableJoined Named, not "
                        + "the join-overload Named (which only pins "
                        + "the processor node, not the subscription "
                        + "topics); those topic names are STILL "
                        + "graph-index-derived, the team believes "
                        + "they fixed the problem and the next "
                        + "topology edit silently orphans both "
                        + "subscription topics; (4) custom "
                        + "partitioners are unset — subscription "
                        + "registrations may land on the wrong "
                        + "partition; TableJoined carries optional "
                        + "StreamPartitioner overrides for both "
                        + "subscription topics; when the LEFT and "
                        + "RIGHT KTables have different upstream "
                        + "partitioning and there is no TableJoined "
                        + "to align them, the subscription "
                        + "registrations may be written to a "
                        + "partition whose owner does not own the "
                        + "corresponding right-side primary key, the "
                        + "RIGHT KTable's task receives a "
                        + "registration it cannot answer, and the "
                        + "subscription response is never produced; "
                        + "the FK join silently drops the join for "
                        + "those left keys (the drop is invisible to "
                        + "the application; the joined KTable just "
                        + "has missing entries); (5) Materialized-"
                        + "only (join(..., Materialized)) fixes the "
                        + "joined-output store name but NOT either "
                        + "subscription topic — same shape as the "
                        + "Named-only half-fix, with the additional "
                        + "wrinkle that the team sees the output-"
                        + "store name pinned in topology.describe() "
                        + "and assumes the join is fully named; the "
                        + "5-arg join(..., Named, Materialized) "
                        + "overload combines both half-fixes and "
                        + "still does NOT pin the subscription "
                        + "topics — only TableJoined does; (6) "
                        + "cross-region replication breaks on the "
                        + "orphans — operations that mirror the "
                        + "application's internal topics (MM2, "
                        + "Confluent Replicator) to another cluster "
                        + "for DR must enumerate the topic names, "
                        + "the orphaned SUBSCRIPTION-REGISTRATION "
                        + "and SUBSCRIPTION-RESPONSE topics for the "
                        + "old graph index are still there and the "
                        + "mirror keeps replicating them "
                        + "indefinitely; the replication target "
                        + "accumulates garbage that nobody will ever "
                        + "consume, on the next topology edit the "
                        + "same thing happens again, after a year "
                        + "of weekly deploys the mirror has tens of "
                        + "orphaned topic pairs; (7) INVOKEDYNAMIC "
                        + "method-reference captures bypass naive "
                        + "MethodInsnNode-only lint — `BiFunction<"
                        + "Function<V, KO>, ValueJoiner<V, VO, VR>, "
                        + "KTable<K, VR>> f = leftTable::join` "
                        + "captures an INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle on "
                        + "KTable.join(KTable, Function, ValueJoiner"
                        + ")KTable; the user-class bytecode contains "
                        + "zero direct INVOKEINTERFACE on the no-"
                        + "TableJoined overload, only the indy site. "
                        + "Migration: pass an explicit TableJoined "
                        + "pinning both subscription topic names — "
                        + "`leftTable.join(rightTable, V::"
                        + "getForeignKey, JOINER, TableJoined.with("
                        + "Named.as(\"my-fk-join\")))`. The "
                        + "TableJoined's Named pins app-id-my-fk-"
                        + "join-subscription-registration-topic and "
                        + "app-id-my-fk-join-subscription-response-"
                        + "topic and survives every topology edit. "
                        + "If the LEFT and RIGHT KTables use custom "
                        + "partitioning, pass StreamPartitioners "
                        + "into the TableJoined as well to pin the "
                        + "subscription partition assignment. The "
                        + "FK join overloads containing TableJoined "
                        + "are never flagged.");
    }
}
