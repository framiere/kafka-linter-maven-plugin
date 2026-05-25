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
 * Fires for every reach of {@link
 * org.apache.kafka.streams.kstream.KStream#join KStream.join},
 * {@link org.apache.kafka.streams.kstream.KStream#leftJoin
 * KStream.leftJoin}, or {@link
 * org.apache.kafka.streams.kstream.KStream#outerJoin
 * KStream.outerJoin} whose descriptor does NOT include ANY of
 * the naming arguments accepted by the safe overloads:
 *
 * <ul>
 *   <li>{@link org.apache.kafka.streams.kstream.StreamJoined
 *       StreamJoined} for KStream-KStream joins,</li>
 *   <li>{@link org.apache.kafka.streams.kstream.Joined Joined}
 *       for KStream-KTable joins,</li>
 *   <li>{@link org.apache.kafka.streams.kstream.Named Named}
 *       for KStream-GlobalKTable joins.</li>
 * </ul>
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface; outerJoin only exists on KStream-KStream)
 * and {@code INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-Named KStream join is THE worst-case unnamed-node
 * hazard</h2>
 *
 * <p>A KStream-KStream join with no {@link
 * org.apache.kafka.streams.kstream.StreamJoined StreamJoined}
 * spawns THREE distinct graph-index-derived internal artifacts:
 *
 * <ol>
 *   <li><b>An auto-repartition topic on each input side</b>
 *       (when either side is key-changing upstream) — e.g.
 *       {@code app-id-KSTREAM-JOINTHIS-0000000005-repartition}
 *       and {@code app-id-KSTREAM-JOINOTHER-0000000007-
 *       repartition}. Same orphan-on-edit hazard as bare
 *       {@code map} but DOUBLED because both sides have one.</li>
 *   <li><b>Two windowed state stores</b> (the THIS-store and
 *       OTHER-store for the join buffer) — e.g. {@code
 *       KSTREAM-JOINTHIS-0000000005-store} and {@code
 *       KSTREAM-JOINOTHER-0000000007-store}. These are RocksDB
 *       state stores on each Streams instance, sized to hold
 *       the entire join window of records on each side.</li>
 *   <li><b>Two state-store changelog topics</b> (one per
 *       state store) — e.g. {@code app-id-KSTREAM-JOINTHIS-
 *       0000000005-store-changelog}. These carry the same
 *       throughput as the input on each side (every input
 *       record produces a changelog entry) and live with full
 *       retention on the brokers.</li>
 * </ol>
 *
 * <p>That is FIVE distinct broker-side artifacts whose names
 * are all derived from the same shifting graph index. A single
 * topology edit upstream of the join renames ALL FIVE, and:
 *
 * <ol>
 *   <li>The two new repartition topics start empty.</li>
 *   <li>The two new state stores are empty → join produces
 *       NULLs for every record falling inside the join window
 *       (which can be hours or days for session/sliding
 *       windows) until they are repopulated from the
 *       (now-also-renamed) changelog topics.</li>
 *   <li>The two new changelog topics also start empty →
 *       there is nothing to restore from. The join is
 *       effectively cold-started, dropping every joined-pair
 *       result that existed before the topology edit.</li>
 *   <li>The OLD five artifacts are orphaned on the brokers
 *       with full retention.</li>
 * </ol>
 *
 * <p>For a real production stream-stream join with a 24-hour
 * window doing 1k records/sec on each side, the consequences
 * of a single no-StreamJoined topology edit are:
 *
 * <ul>
 *   <li>~170 GB of orphan repartition + changelog data per
 *       broker, per topology edit, accumulating linearly with
 *       edit count;</li>
 *   <li>24 hours of NULL join results during which downstream
 *       consumers (alerting, billing, dashboards) see no
 *       events for any joined keys;</li>
 *   <li>permanent loss of every joined pair that was produced
 *       in the 24-hour window before the edit.</li>
 * </ul>
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Quarterly compliance audit silently breaks.</b>
 *       Team has {@code orders.join(payments, (o, p) ->
 *       reconcile(o, p), JoinWindows.of(Duration.ofHours(24)))}
 *       — a reconciliation join with a 24-hour window producing
 *       reconciled-order events into a sink topic that feeds
 *       quarterly compliance reports. After one upstream
 *       topology edit (renaming the {@code orders} source's
 *       upstream {@code map} to {@code mapValues}, say), the
 *       JOINTHIS / JOINOTHER state stores are renamed; Streams
 *       starts the new join from empty state stores; the
 *       reconciled-orders sink topic shows 24 hours of zero
 *       events; the next quarterly compliance report shows zero
 *       reconciled orders for that 24-hour window; SOX reviewer
 *       flags it; engineering eventually traces it to the
 *       topology edit, but the lost data cannot be recovered
 *       because the original JOINTHIS/JOINOTHER changelog
 *       topics were orphaned (and have since rolled off
 *       retention).</li>
 *   <li><b>Five-artifact orphan accumulation per topology
 *       edit.</b> Each topology edit upstream of a KStream-
 *       KStream join leaves behind: 1 old JOINTHIS repartition
 *       topic + 1 old JOINOTHER repartition topic + 1 old
 *       JOINTHIS-store + 1 old JOINOTHER-store + 1 old
 *       JOINTHIS-store-changelog + 1 old JOINOTHER-store-
 *       changelog = 5 broker-side artifacts (excluding the
 *       state stores which are local to Streams instances).
 *       For an org with 50 stream-stream joins across its
 *       Kafka Streams fleet and an average of 8 topology edits
 *       per join per year, that's 50 × 8 × 5 = 2000 orphan
 *       broker-side artifacts per year — each with its own
 *       retention-bound disk footprint, replication overhead,
 *       and metadata cost.</li>
 *   <li><b>Join-throughput dashboards rebrand on every
 *       edit.</b> Grafana panels showing {@code
 *       kafka.streams:type=stream-processor-node-metrics,
 *       processor-node-id=KSTREAM-JOINTHIS-0000000005}
 *       process-rate, plus the join-window late-records-dropped
 *       counter, plus the join-window expired-records counter,
 *       plus the joined-pairs-per-second sink-topic metric —
 *       ALL rebrand on every edit. Capacity planning for
 *       windowed joins becomes a guessing game.</li>
 *   <li><b>Cold-start join recovery requires manual changelog
 *       replay.</b> Even when SRE notices the join is producing
 *       no output, the recovery procedure is: stop Streams,
 *       reset offsets to the start of the input topics on both
 *       sides, restart and wait for the join window to refill.
 *       For a 24-hour window doing 1k records/sec on each side
 *       this is ~170 GB of input replay; the brokers handle
 *       2x normal join-input throughput during the replay
 *       window AND have to write to the new (empty) state-store
 *       changelogs at the same rate; rolling-restart-aware
 *       deploys multiply this cost.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       join factory built as a method reference {@code
 *       stream::join} bound to a custom 3-arg SAM whose erased
 *       descriptor matches one of the no-naming overloads
 *       compiles to an {@code INVOKEDYNAMIC} site whose
 *       bsm-args contain a {@code REF_invokeInterface} Handle
 *       pointing at {@code KStream.join(KStream, ValueJoiner,
 *       JoinWindows)KStream}. The user-class bytecode contains
 *       zero direct {@code INVOKEINTERFACE} on the no-Named
 *       overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.StreamJoined StreamJoined}
 * naming the join state stores AND the repartition topics —
 * e.g. {@code orders.join(payments, this::reconcile,
 * JoinWindows.of(Duration.ofHours(24)),
 * StreamJoined.as("orders-payments-reconcile-join"))}. The
 * explicit name flows into ALL FIVE artifacts (both
 * repartition topics, both state stores, both changelog
 * topics), stabilizing them across topology edits. For
 * KStream-KTable joins use {@link
 * org.apache.kafka.streams.kstream.Joined Joined.as(...)}; for
 * KStream-GlobalKTable joins use {@link
 * org.apache.kafka.streams.kstream.Named Named.as(...)}.
 */
public final class StreamsStreamJoinNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final Set<String> METHOD_NAMES = Set.of("join", "leftJoin", "outerJoin");
    private static final String STREAM_JOINED_TOKEN =
            "Lorg/apache/kafka/streams/kstream/StreamJoined;";
    private static final String JOINED_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Joined;";
    private static final String NAMED_TOKEN =
            "Lorg/apache/kafka/streams/kstream/Named;";

    private final Severity severity;

    public StreamsStreamJoinNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_STREAM_JOIN_NO_NAMED;
    }

    private static boolean isUnsafe(String desc) {
        return desc != null
                && !desc.contains(STREAM_JOINED_TOKEN)
                && !desc.contains(JOINED_TOKEN)
                && !desc.contains(NAMED_TOKEN);
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
                    Handle h = findUnsafeJoinHandle(indy);
                    if (h != null) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private static Handle findUnsafeJoinHandle(InvokeDynamicInsnNode indy) {
        for (String name : METHOD_NAMES) {
            Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, name, null);
            if (h != null && isUnsafe(h.getDesc())) {
                return h;
            }
        }
        return null;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_STREAM_JOIN_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.join / leftJoin / outerJoin — a no-naming "
                        + "overload is reached here (no StreamJoined "
                        + "for KStream-KStream, no Joined for KStream-"
                        + "KTable, no Named for KStream-GlobalKTable) "
                        + "— either as a direct INVOKEINTERFACE on the "
                        + "method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `stream::join` "
                        + "bound to a custom 3-arg SAM whose erased "
                        + "implMethod descriptor matches an unsafe "
                        + "overload). A KStream-KStream join with no "
                        + "StreamJoined is THE WORST-CASE unnamed-node "
                        + "hazard: it spawns FIVE distinct graph-index-"
                        + "derived broker-side artifacts: (a) a "
                        + "JOINTHIS auto-repartition topic; (b) a "
                        + "JOINOTHER auto-repartition topic; (c) a "
                        + "JOINTHIS windowed state store; (d) a "
                        + "JOINOTHER windowed state store; (e) two "
                        + "state-store changelog topics (one per "
                        + "store). All five are derived from the same "
                        + "shifting topology graph index — a single "
                        + "topology edit upstream of the join renames "
                        + "ALL FIVE, and: (1) the two new repartition "
                        + "topics start empty, (2) the two new state "
                        + "stores are empty so the join produces NULLs "
                        + "for every record falling inside the join "
                        + "window (hours-to-days for session/sliding "
                        + "windows), (3) the two new changelog topics "
                        + "also start empty so there is nothing to "
                        + "restore from — the join is effectively "
                        + "cold-started, dropping every joined-pair "
                        + "result that existed before the topology "
                        + "edit, (4) the OLD five artifacts are "
                        + "orphaned on the brokers with full "
                        + "retention. Concrete failure modes: (1) "
                        + "quarterly compliance audit silently breaks "
                        + "— team has orders.join(payments, (o, p) -> "
                        + "reconcile(o, p), JoinWindows.of(Duration.of"
                        + "Hours(24))) feeding quarterly compliance "
                        + "reports; after one upstream topology edit "
                        + "the JOINTHIS/JOINOTHER state stores are "
                        + "renamed, Streams starts the new join from "
                        + "empty state stores, the reconciled-orders "
                        + "sink shows 24 hours of zero events, the "
                        + "next quarterly compliance report shows zero "
                        + "reconciled orders for that 24-hour window, "
                        + "SOX reviewer flags it, engineering "
                        + "eventually traces it to the topology edit "
                        + "but the lost data cannot be recovered "
                        + "because the original JOINTHIS/JOINOTHER "
                        + "changelog topics were orphaned and have "
                        + "since rolled off retention; (2) five-"
                        + "artifact orphan accumulation per topology "
                        + "edit — each topology edit upstream of a "
                        + "KStream-KStream join leaves behind 1 old "
                        + "JOINTHIS repartition + 1 old JOINOTHER "
                        + "repartition + 1 old JOINTHIS-store "
                        + "changelog + 1 old JOINOTHER-store changelog "
                        + "= 5 broker-side artifacts (plus per-"
                        + "instance state-store disk); for an org "
                        + "with 50 stream-stream joins and an average "
                        + "of 8 topology edits per join per year, "
                        + "that's 50 * 8 * 5 = 2000 orphan broker-"
                        + "side artifacts per year; (3) join-"
                        + "throughput dashboards rebrand on every "
                        + "edit — Grafana panels on processor-node-id"
                        + "=KSTREAM-JOINTHIS-0000000005 process-rate, "
                        + "join-window late-records-dropped, join-"
                        + "window expired-records, joined-pairs-per-"
                        + "second ALL rebrand; capacity planning for "
                        + "windowed joins becomes a guessing game; "
                        + "(4) cold-start join recovery requires "
                        + "manual changelog replay — even when SRE "
                        + "notices the join is producing no output, "
                        + "the recovery procedure is to stop Streams, "
                        + "reset offsets to the start of the input "
                        + "topics on both sides, restart and wait for "
                        + "the join window to refill; for a 24-hour "
                        + "window doing 1k records/sec on each side "
                        + "this is ~170 GB of input replay, brokers "
                        + "handle 2x normal join-input throughput "
                        + "during the replay window AND have to write "
                        + "to the new (empty) state-store changelogs "
                        + "at the same rate; rolling-restart-aware "
                        + "deploys multiply this cost; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — a "
                        + "join factory built as `stream::join` bound "
                        + "to a custom 3-arg SAM whose erased "
                        + "descriptor matches one of the no-naming "
                        + "overloads compiles to INVOKEDYNAMIC whose "
                        + "bsm-args contain a REF_invokeInterface "
                        + "Handle pointing at KStream.join(KStream, "
                        + "ValueJoiner, JoinWindows)KStream; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit StreamJoined naming the join "
                        + "state stores AND the repartition topics — "
                        + "`orders.join(payments, this::reconcile, "
                        + "JoinWindows.of(Duration.ofHours(24)), "
                        + "StreamJoined.as(\"orders-payments-"
                        + "reconcile-join\"))`. The explicit name "
                        + "flows into ALL FIVE artifacts (both "
                        + "repartition topics, both state stores, "
                        + "both changelog topics), stabilizing them "
                        + "across topology edits. For KStream-KTable "
                        + "joins use Joined.as(...); for KStream-"
                        + "GlobalKTable joins use Named.as(...). The "
                        + "join/leftJoin/outerJoin overloads containing "
                        + "StreamJoined / Joined / Named are never "
                        + "flagged.");
    }
}
