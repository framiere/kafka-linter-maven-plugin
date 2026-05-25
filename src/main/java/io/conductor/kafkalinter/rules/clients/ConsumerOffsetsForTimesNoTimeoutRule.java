package io.conductor.kafkalinter.rules.clients;

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
 * Fires for every reach of the unbounded
 * {@link org.apache.kafka.clients.consumer.Consumer#offsetsForTimes(java.util.Map)}
 * overload — descriptor {@code (Ljava/util/Map;)Ljava/util/Map;} with
 * no Duration argument. Catches both direct
 * {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE} calls and indirect
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code consumer::offsetsForTimes} bound to a
 * {@link java.util.function.Function} or
 * {@link java.util.concurrent.Callable}) via a dual walk over each
 * method's instructions.
 *
 * <h2>Why no-Duration offsetsForTimes is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#offsetsForTimes(java.util.Map)}
 * is documented as equivalent to {@code offsetsForTimes(
 * timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s. The call sends a
 * ListOffsetsForTimestamp request to the leader of each partition
 * asking for the earliest offset whose record timestamp is ≥ the
 * caller-supplied target, with no caller-side deadline tighter than
 * 60 s. The lookup is more expensive than a plain
 * earliest/latest-offset query because the broker has to perform a
 * timestamp-indexed scan of the partition's segments — each segment
 * may need its time-index file consulted, and segments without a
 * time-index (very old logs, custom segment formats) require a
 * full-segment scan. The leader can be:
 *
 * <ul>
 *   <li><b>Unavailable</b> — leader election in progress, broker
 *       rolling restart, ISR shrinking, network partition. The
 *       Consumer's metadata-refresh logic retries until
 *       {@code default.api.timeout.ms} expires; only then does a
 *       {@link org.apache.kafka.common.errors.TimeoutException}
 *       surface.</li>
 *   <li><b>Slow</b> — disk pressure on the time-index files (large
 *       segments, recent log rotation, mmap eviction), OS-level
 *       page-cache eviction, broker GC pause. The full 60 s budget
 *       is consumed per call.</li>
 *   <li><b>Stale-metadata-targeted</b> — the consumer's cached
 *       metadata points at an old leader; the request fails with
 *       {@code NotLeaderOrFollowerException}, metadata is refreshed
 *       and the request is retried against the new leader, all
 *       under the same 60 s budget.</li>
 * </ul>
 *
 * <p>Five concrete failure modes (carried verbatim into the violation
 * message so the engineer reading the lint report understands the
 * "why" without leaving the IDE):
 *
 * <ul>
 *   <li><b>point-in-time replay tooling appears 'stuck' during
 *       exactly the incidents that prompt replay.</b> Pattern: an
 *       operator chooses to replay from "10 minutes before the
 *       incident"; tooling calls
 *       {@code offsetsForTimes(Map.of(p1, t, p2, t, ...))} to map
 *       each partition's timestamp to an offset, then
 *       {@code seek(p, offsets.get(p).offset())} + replay. Under
 *       broker slowness the {@code offsetsForTimes} call blocks for
 *       the full 60 s and the operator cannot distinguish "tool is
 *       hung" from "cluster is slow" — incident response slows
 *       down.</li>
 *   <li><b>scheduled retention/audit jobs pin scheduler threads.</b>
 *       Pattern: a nightly audit job calls
 *       {@code offsetsForTimes} with a per-partition target
 *       timestamp (e.g. "the offset at midnight UTC") to confirm
 *       retention windows. Under broker outage the job stalls for
 *       60 s per call; downstream jobs queue behind it; the audit
 *       window misses its SLO.</li>
 *   <li><b>compliance / lineage tooling falsely reports 'data not
 *       found' on slow brokers.</b> Pattern: a regulatory query
 *       maps a customer-supplied "record produced at time T" to
 *       offsets via {@code offsetsForTimes}; the no-Duration
 *       overload blocks for 60 s on a slow broker; the upstream
 *       HTTP request times out at a lower (typically 5-10 s)
 *       deadline and the consumer wrapper returns null — the
 *       lineage tool reports "no record at this timestamp" when in
 *       fact the broker was simply slow.</li>
 *   <li><b>scheduler-thread pinning when {@code offsetsForTimes} is
 *       dispatched via {@link java.util.concurrent.ScheduledExecutorService}
 *       method-reference capture.</b> A Callable-typed indy capture
 *       {@code executor.submit((Callable) consumer::offsetsForTimes)}
 *       pins the executor worker thread for 60 s on broker outage;
 *       backlogged tasks (heartbeats, metric emission, health
 *       probes) queue behind it; the executor's queue saturates and
 *       tasks are silently dropped.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::offsetsForTimes} captures
 *       bypass naïve MethodInsnNode-only lint.</b> A
 *       {@link java.util.function.Function} or
 *       {@link java.util.concurrent.Callable} parameter bound by a
 *       {@code consumer::offsetsForTimes} method reference compiles
 *       to {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} (KafkaConsumer typed receiver) or
 *       {@code REF_invokeInterface} (Consumer interface typed
 *       receiver) handle pointing at
 *       {@code Consumer.offsetsForTimes(Map)}. The user-class
 *       bytecode contains zero direct {@code INVOKEVIRTUAL} on the
 *       no-Duration overload — only the indy site. A rule that
 *       walks only {@code MethodInsnNode} misses every such
 *       site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — one no-Duration overload, one
 * bounded overload</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares two
 * offsetsForTimes overloads with different descriptors:
 *
 * <ul>
 *   <li>Unbounded: {@code (Ljava/util/Map;)Ljava/util/Map;} —
 *       offsetsForTimes(Map)</li>
 *   <li>Bounded:
 *       {@code (Ljava/util/Map;Ljava/time/Duration;)Ljava/util/Map;}
 *       — offsetsForTimes(Map, Duration)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the no-Duration descriptor; the
 * bounded overload has a strictly different signature and is never
 * flagged.
 */
public final class ConsumerOffsetsForTimesNoTimeoutRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "offsetsForTimes";
    private static final String NO_TIMEOUT_DESC = "(Ljava/util/Map;)Ljava/util/Map;";

    private final Severity severity;

    public ConsumerOffsetsForTimesNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_OFFSETS_FOR_TIMES_NO_TIMEOUT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && NO_TIMEOUT_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, NO_TIMEOUT_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.CONSUMER_OFFSETS_FOR_TIMES_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.offsetsForTimes(Map) (no Duration) is reached "
                        + "here — either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::offsetsForTimes` bound to a "
                        + "Function or Callable SAM, common shapes when "
                        + "point-in-time replay tooling or scheduled "
                        + "audit jobs dispatch timestamp-to-offset reads "
                        + "via ScheduledExecutorService). The no-Duration "
                        + "overload is documented as equivalent to "
                        + "offsetsForTimes(timestampsToSearch, "
                        + "Duration.ofMillis(defaultApiTimeoutMs)) where "
                        + "default.api.timeout.ms defaults to 60 s — the "
                        + "ListOffsetsForTimestamp request is retried "
                        + "against the partition leader until that budget "
                        + "expires, with no shorter caller-side bound "
                        + "available. The lookup is more expensive than a "
                        + "plain earliest/latest-offset query because the "
                        + "broker has to perform a timestamp-indexed scan "
                        + "of the partition's segments — each segment may "
                        + "need its time-index file consulted, and "
                        + "segments without a time-index (very old logs, "
                        + "custom segment formats) require a "
                        + "full-segment scan. Under leader unavailability "
                        + "(leader election, broker rolling restart, ISR "
                        + "shrinking, client/leader network partition), "
                        + "leader slowness (disk pressure on the "
                        + "time-index files for large segments, recent "
                        + "log rotation, mmap eviction, OS-level "
                        + "page-cache eviction, broker GC pause), or "
                        + "stale-metadata retargeting "
                        + "(NotLeaderOrFollowerException triggers a "
                        + "metadata refresh and a retry against the new "
                        + "leader under the same 60 s budget), the call "
                        + "blocks for the full default.api.timeout.ms. "
                        + "Five concrete failure modes follow: (1) "
                        + "point-in-time replay tooling appears 'stuck' "
                        + "during exactly the incidents that prompt "
                        + "replay — an operator chooses to replay from "
                        + "'10 minutes before the incident'; tooling "
                        + "calls `offsetsForTimes(Map.of(p1, t, p2, t, "
                        + "...))` to map each partition's timestamp to "
                        + "an offset, then `seek(p, "
                        + "offsets.get(p).offset())` + replay; under "
                        + "broker slowness the `offsetsForTimes` call "
                        + "blocks for the full 60 s and the operator "
                        + "cannot distinguish 'tool is hung' from "
                        + "'cluster is slow', slowing incident response; "
                        + "(2) scheduled retention/audit jobs pin "
                        + "scheduler threads — a nightly audit job calls "
                        + "`offsetsForTimes` with a per-partition target "
                        + "timestamp (e.g. 'the offset at midnight UTC') "
                        + "to confirm retention windows; under broker "
                        + "outage the job stalls for 60 s per call, "
                        + "downstream jobs queue behind it, the audit "
                        + "window misses its SLO; (3) compliance / "
                        + "lineage tooling falsely reports 'data not "
                        + "found' on slow brokers — a regulatory query "
                        + "maps a customer-supplied 'record produced at "
                        + "time T' to offsets via `offsetsForTimes`; the "
                        + "no-Duration overload blocks for 60 s on a "
                        + "slow broker; the upstream HTTP request times "
                        + "out at a lower (typically 5-10 s) deadline "
                        + "and the consumer wrapper returns null — the "
                        + "lineage tool reports 'no record at this "
                        + "timestamp' when in fact the broker was simply "
                        + "slow; (4) scheduler-thread pinning when "
                        + "`offsetsForTimes` is dispatched via "
                        + "ScheduledExecutorService method-reference "
                        + "capture — a Callable-typed indy capture "
                        + "`executor.submit((Callable) "
                        + "consumer::offsetsForTimes)` pins the executor "
                        + "worker thread for 60 s on broker outage; "
                        + "backlogged tasks (heartbeats, metric "
                        + "emission, health probes) queue behind it; "
                        + "the executor's queue saturates and tasks are "
                        + "silently dropped; (5) INVOKEDYNAMIC "
                        + "`consumer::offsetsForTimes` captures bypass "
                        + "naïve MethodInsnNode-only lint — a Function "
                        + "or Callable parameter bound by a "
                        + "`consumer::offsetsForTimes` method reference "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeVirtual or "
                        + "REF_invokeInterface handle pointing at "
                        + "Consumer.offsetsForTimes(Map); the user-class "
                        + "bytecode contains zero direct INVOKEVIRTUAL "
                        + "on the no-Duration overload, only the indy "
                        + "site. Migration: use the bounded overload "
                        + "`offsetsForTimes(Map, Duration)` matched to "
                        + "the surrounding deadline (HTTP request "
                        + "deadline, audit-window SLO, scheduler task "
                        + "budget) and let the TimeoutException surface "
                        + "a broker-outage error rather than an "
                        + "indefinite-feeling 60 s hang. The bounded "
                        + "overload has descriptor "
                        + "`(Ljava/util/Map;Ljava/time/Duration;)"
                        + "Ljava/util/Map;` and is never flagged by this "
                        + "rule.");
    }
}
