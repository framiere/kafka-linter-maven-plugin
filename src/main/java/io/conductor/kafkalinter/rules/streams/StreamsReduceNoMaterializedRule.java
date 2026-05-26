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
 * Fires for every reach of any overload of {@code reduce} on
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
 * <p>Predicate is structural — any descriptor for {@code reduce}
 * on the four grouped owner interfaces whose argument list does
 * NOT contain {@code org/apache/kafka/streams/kstream/
 * Materialized} is unsafe. That captures the canonical
 * {@code reduce(Reducer)} overload on KGroupedStream /
 * TimeWindowedKStream / SessionWindowedKStream, and the
 * KGroupedTable adder-subtractor form {@code reduce(Reducer,
 * Reducer)}, but NOT any overload that includes a {@code
 * Materialized} argument. Unlike {@code count}, the Kafka API
 * does NOT expose a standalone {@code Named}-only half-fix for
 * {@code reduce} — every Named overload is paired with
 * Materialized — so the rule only filters on Materialized
 * presence.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (all
 * four owners are interfaces) and {@code INVOKEDYNAMIC} method-
 * reference captures (e.g. {@code grouped::reduce} bound to a
 * custom SAM whose erased implMethod descriptor matches an
 * unsafe overload).
 *
 * <h2>Why reduce — same-type replacement — still bites when
 * the state store is unnamed</h2>
 *
 * <p>Of the three aggregations ({@code count}, {@code reduce},
 * {@code aggregate}), {@code reduce} sits in the middle of the
 * per-key state-size spectrum: it carries one value of the input
 * type {@code V} per key (vs. {@code Long} for {@code count}, vs.
 * a custom user-defined {@code VR} for {@code aggregate}).
 * Typical {@code reduce} usage maintains a running sum, max, min,
 * latest, or merged record per key — losing it on a topology edit
 * silently re-derives the reduction from a single incoming record
 * after the rename, masquerading as a working but completely
 * wrong result for any consumer of the changelog or downstream
 * KTable.
 *
 * <p>When no {@link
 * org.apache.kafka.streams.kstream.Materialized Materialized} is
 * passed, the state store AND its companion changelog topic are
 * auto-named from the topology graph index — e.g. {@code
 * KSTREAM-REDUCE-STATE-STORE-0000000007} and {@code app-id-
 * KSTREAM-REDUCE-STATE-STORE-0000000007-changelog}. Any topology
 * edit upstream renames the state store and the changelog; the
 * new state store starts empty; the first input record after the
 * deploy becomes the only seed for the reduction (because {@link
 * org.apache.kafka.streams.kstream.Reducer Reducer} has NO
 * initializer — it bootstraps lazily on the first record per
 * key); the OLD changelog topic is orphaned on broker disk with
 * full retention.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Running max / min silently resets to the first record
 *       after deploy.</b> Team has {@code prices.groupByKey()
 *       .reduce((a, b) -> Math.max(a, b))} tracking per-symbol
 *       max-of-day. After one upstream topology edit the graph
 *       index shifts; Streams creates a NEW state store and a
 *       NEW changelog topic (both empty); on the next incoming
 *       trade the reducer initializes the per-key value to THAT
 *       trade's price (because Reducer has no initializer);
 *       max-of-day is now "max of all trades since the last
 *       topology edit," which is identical to "current price"
 *       for any key that hasn't seen a higher trade yet. The
 *       downstream consumer (a risk dashboard) sees a max that
 *       is mathematically a subset of the true max — no error
 *       fires, no alert triggers, the dashboard is just wrong
 *       in the conservative direction.</li>
 *   <li><b>KGroupedTable.reduce(adder, subtractor) loses BOTH
 *       directions of the running net.</b> A KGroupedTable
 *       reduce maintains a net per key by running the adder on
 *       the new value and the subtractor on the previous value
 *       for every input update. After a topology-edit rename
 *       the state store is empty; the next input update is
 *       treated as a fresh add against an empty initial value;
 *       the subtractor never fires for the formerly-tracked
 *       prior values; the net diverges from reality with no
 *       record of when divergence started. Recovery requires
 *       replaying the upstream KTable's full changelog (a
 *       compacted topic — only the latest value per key is
 *       retained — meaning the subtract-then-add transition
 *       history is unrecoverable; the net can only be
 *       re-derived as a clean re-add from the compacted
 *       snapshot, which discards every intermediate state).</li>
 *   <li><b>Changelog topic orphan accumulation.</b> Each
 *       topology edit leaves the previous reduce's changelog
 *       ({@code app-id-KSTREAM-REDUCE-STATE-STORE-0000000007-
 *       changelog}) orphaned with full retention. For a reduce
 *       maintaining a 200-byte average record per key against
 *       10k keys and 5k updates/sec, the changelog runs at
 *       ~1 MB/sec — 84 GB per active day. An org with 30
 *       reduce(no-Materialized) call sites and 3 topology
 *       edits per site per year accumulates ~90 orphan
 *       changelog topics per year, each carrying full
 *       retention.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       reduce factory built as a custom 1-arg SAM {@code
 *       ReduceFactory<K, V>} bound to {@code grouped::reduce}
 *       compiles to {@code INVOKEDYNAMIC} whose bsm-args
 *       contain a {@code REF_invokeInterface} Handle pointing
 *       at {@code KGroupedStream.reduce(Reducer)KTable}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Materialized overload, only
 *       the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Materialized Materialized}
 * naming the state store — e.g. {@code grouped.reduce(Math::max,
 * Materialized.as("max-of-day-store"))}. The explicit name pins
 * BOTH the state store name AND the changelog topic name
 * ({@code app-id-max-of-day-store-changelog}). For full naming
 * coverage (state store + processor node + changelog), use the
 * 3-arg overload {@code reduce(Reducer, Named, Materialized)} on
 * KGroupedStream / TimeWindowedKStream / SessionWindowedKStream,
 * or the 4-arg {@code reduce(Reducer, Reducer, Named,
 * Materialized)} on KGroupedTable.
 */
public final class StreamsReduceNoMaterializedRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.GROUPED_KSTREAM_OWNERS;
    private static final String METHOD_NAME = "reduce";
    private static final String MATERIALIZED_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Materialized;";

    private final Severity severity;

    public StreamsReduceNoMaterializedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_REDUCE_NO_MATERIALIZED;
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
                RuleId.STREAMS_REDUCE_NO_MATERIALIZED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KGroupedStream / KGroupedTable / TimeWindowed"
                        + "KStream / SessionWindowedKStream . "
                        + "reduce — a no-Materialized overload is "
                        + "reached here (the canonical "
                        + "reduce(Reducer) form, or KGroupedTable's "
                        + "adder-subtractor reduce(Reducer, Reducer) "
                        + "form — none include Materialized) — "
                        + "either as a direct INVOKEINTERFACE on the "
                        + "method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `grouped::reduce` "
                        + "bound to a custom SAM whose erased "
                        + "implMethod descriptor matches an unsafe "
                        + "overload). Of the three aggregations "
                        + "(count, reduce, aggregate), reduce sits "
                        + "in the middle of the per-key state-size "
                        + "spectrum: it carries one value of the "
                        + "input type V per key (vs. Long for count, "
                        + "vs. a custom user-defined VR for "
                        + "aggregate). Typical reduce usage "
                        + "maintains a running sum, max, min, "
                        + "latest, or merged record per key — losing "
                        + "it on a topology edit silently re-derives "
                        + "the reduction from a single incoming "
                        + "record after the rename, masquerading as "
                        + "a working but completely wrong result for "
                        + "any consumer of the changelog or "
                        + "downstream KTable. When no Materialized "
                        + "is passed, the state store AND its "
                        + "companion changelog topic are auto-named "
                        + "from the topology graph index (e.g. "
                        + "KSTREAM-REDUCE-STATE-STORE-0000000007 and "
                        + "app-id-KSTREAM-REDUCE-STATE-STORE-"
                        + "0000000007-changelog). Any topology edit "
                        + "upstream renames the state store and the "
                        + "changelog; the new state store starts "
                        + "empty; the first input record after the "
                        + "deploy becomes the only seed for the "
                        + "reduction (because Reducer has NO "
                        + "initializer — it bootstraps lazily on the "
                        + "first record per key); the OLD changelog "
                        + "topic is orphaned on broker disk with "
                        + "full retention. Concrete failure modes: "
                        + "(1) running max / min silently resets to "
                        + "the first record after deploy — team has "
                        + "prices.groupByKey().reduce((a, b) -> "
                        + "Math.max(a, b)) tracking per-symbol max-"
                        + "of-day, after one upstream topology edit "
                        + "the graph index shifts, Streams creates "
                        + "a NEW state store and a NEW changelog "
                        + "topic (both empty), on the next incoming "
                        + "trade the reducer initializes the per-"
                        + "key value to THAT trade's price (because "
                        + "Reducer has no initializer), max-of-day "
                        + "is now \"max of all trades since the "
                        + "last topology edit\" which is identical "
                        + "to \"current price\" for any key that "
                        + "hasn't seen a higher trade yet, the "
                        + "downstream consumer (a risk dashboard) "
                        + "sees a max that is mathematically a "
                        + "subset of the true max — no error fires, "
                        + "no alert triggers, the dashboard is just "
                        + "wrong in the conservative direction; (2) "
                        + "KGroupedTable.reduce(adder, subtractor) "
                        + "loses BOTH directions of the running net "
                        + "— a KGroupedTable reduce maintains a net "
                        + "per key by running the adder on the new "
                        + "value and the subtractor on the previous "
                        + "value for every input update, after a "
                        + "topology-edit rename the state store is "
                        + "empty, the next input update is treated "
                        + "as a fresh add against an empty initial "
                        + "value, the subtractor never fires for "
                        + "the formerly-tracked prior values, the "
                        + "net diverges from reality with no record "
                        + "of when divergence started, recovery "
                        + "requires replaying the upstream KTable's "
                        + "full changelog (a compacted topic — only "
                        + "the latest value per key is retained — "
                        + "meaning the subtract-then-add transition "
                        + "history is unrecoverable, the net can "
                        + "only be re-derived as a clean re-add "
                        + "from the compacted snapshot which "
                        + "discards every intermediate state); (3) "
                        + "changelog topic orphan accumulation — "
                        + "each topology edit leaves the previous "
                        + "reduce's changelog orphaned with full "
                        + "retention, for a reduce maintaining a "
                        + "200-byte average record per key against "
                        + "10k keys and 5k updates/sec the "
                        + "changelog runs at ~1 MB/sec or 84 GB "
                        + "per active day, an org with 30 reduce"
                        + "(no-Materialized) call sites and 3 "
                        + "topology edits per site per year "
                        + "accumulates ~90 orphan changelog topics "
                        + "per year, each carrying full retention; "
                        + "(4) INVOKEDYNAMIC method-reference "
                        + "captures bypass naive MethodInsnNode-"
                        + "only lint — a reduce factory built as a "
                        + "custom 1-arg SAM ReduceFactory<K, V> "
                        + "bound to grouped::reduce compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KGroupedStream.reduce(Reducer)KTable; "
                        + "the user-class bytecode contains zero "
                        + "direct INVOKEINTERFACE on the no-"
                        + "Materialized overload, only the indy "
                        + "site. Migration: pass an explicit "
                        + "Materialized naming the state store — "
                        + "`grouped.reduce(Math::max, Materialized."
                        + "as(\"max-of-day-store\"))`. The explicit "
                        + "name pins BOTH the state store name AND "
                        + "the changelog topic name (app-id-max-of-"
                        + "day-store-changelog). For full naming "
                        + "coverage (state store + processor node + "
                        + "changelog), use the 3-arg reduce(Reducer, "
                        + "Named, Materialized) overload on "
                        + "KGroupedStream / TimeWindowedKStream / "
                        + "SessionWindowedKStream, or the 4-arg "
                        + "reduce(Reducer, Reducer, Named, "
                        + "Materialized) on KGroupedTable. The "
                        + "reduce overloads containing Materialized "
                        + "are never flagged.");
    }
}
