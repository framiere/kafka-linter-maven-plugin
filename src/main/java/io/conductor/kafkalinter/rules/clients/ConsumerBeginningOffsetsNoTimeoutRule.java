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
 * {@link org.apache.kafka.clients.consumer.Consumer#beginningOffsets(java.util.Collection)}
 * overload — descriptor {@code (Ljava/util/Collection;)Ljava/util/Map;}
 * with no Duration argument. Catches both direct
 * {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE} calls and indirect
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code consumer::beginningOffsets} bound to a
 * {@link java.util.function.Function} or
 * {@link java.util.concurrent.Callable}) via a dual walk over each
 * method's instructions.
 *
 * <h2>Why no-Duration beginningOffsets is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#beginningOffsets(java.util.Collection)}
 * is documented as equivalent to {@code beginningOffsets(partitions,
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s. The call sends a
 * ListOffsets request to the leader of each partition asking for the
 * earliest offset, with no caller-side deadline tighter than 60 s.
 * The leader can be:
 *
 * <ul>
 *   <li><b>Unavailable</b> — leader election in progress, broker
 *       rolling restart, ISR shrinking, network partition between the
 *       client and the leader. The Consumer's metadata-refresh logic
 *       retries until {@code default.api.timeout.ms} expires; only
 *       then does a {@link org.apache.kafka.common.errors.TimeoutException}
 *       surface, with no shorter caller-side bound available.</li>
 *   <li><b>Slow</b> — disk pressure on the segment storing the
 *       earliest offset (especially after a log-retention cycle has
 *       just rolled the earliest segment), OS-level page-cache
 *       eviction, broker GC pause. The full 60 s budget is consumed
 *       per call.</li>
 *   <li><b>Stale-metadata-targeted</b> — the consumer's cached
 *       metadata points at an old leader; the request fails with
 *       {@code NotLeaderOrFollowerException}, metadata is refreshed
 *       and the request is retried against the new leader, all under
 *       the same 60 s budget.</li>
 * </ul>
 *
 * <p>Five concrete failure modes (carried verbatim into the violation
 * message so the engineer reading the lint report understands the
 * "why" without leaving the IDE):
 *
 * <ul>
 *   <li><b>retention / log-truncation monitors pin threads during
 *       the exact incidents they exist to detect.</b> Pattern: a
 *       compliance / SLA monitor polls
 *       {@code consumer.beginningOffsets(assignment)} every N
 *       seconds and alerts when the earliest offset advances past a
 *       configured threshold (indicating log-retention is deleting
 *       un-replayed data). Under a broker outage the call blocks for
 *       the full 60 s; subsequent ticks queue behind it; the monitor
 *       goes silent during the exact retention-storm incident it
 *       exists to detect.</li>
 *   <li><b>replay-from-earliest tools appear 'stuck' during exactly
 *       the incidents that prompt replay.</b> A common operator
 *       workflow is: discover an issue, call {@code beginningOffsets}
 *       to discover the earliest available offset (so the seek does
 *       not target an offset that retention has already deleted),
 *       then {@code seek} + replay. Under broker slowness the first
 *       {@code beginningOffsets} call blocks for 60 s and the
 *       operator cannot distinguish "tool is hung" from "cluster is
 *       slow" — incident response slows down.</li>
 *   <li><b>health-probe blocking pins kubelet liveness/readiness
 *       paths.</b> An HTTP readiness endpoint that calls
 *       {@code beginningOffsets} to confirm broker reachability
 *       blocks the handler thread for 60 s under broker outage; the
 *       kubelet probe timeout (typically 1-3 s) fires and the Pod is
 *       restarted, masking the root-cause outage behind a thrashing
 *       restart loop.</li>
 *   <li><b>scheduler-thread pinning when {@code beginningOffsets} is
 *       dispatched via {@link java.util.concurrent.ScheduledExecutorService}
 *       method-reference capture.</b> A Callable-typed indy capture
 *       {@code executor.submit((Callable) consumer::beginningOffsets)}
 *       pins the executor worker thread for 60 s on broker outage;
 *       backlogged tasks queue behind it; the executor's queue
 *       saturates and tasks are silently dropped.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::beginningOffsets} captures
 *       bypass naïve MethodInsnNode-only lint.</b> A
 *       {@link java.util.function.Function} or
 *       {@link java.util.concurrent.Callable} parameter bound by a
 *       {@code consumer::beginningOffsets} method reference compiles
 *       to {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} (KafkaConsumer typed receiver) or
 *       {@code REF_invokeInterface} (Consumer interface typed
 *       receiver) handle pointing at
 *       {@code Consumer.beginningOffsets(Collection)}. The
 *       user-class bytecode contains zero direct
 *       {@code INVOKEVIRTUAL} on the no-Duration overload — only the
 *       indy site. A rule that walks only {@code MethodInsnNode}
 *       misses every such site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — one no-Duration overload, one
 * bounded overload</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares two
 * beginningOffsets overloads with different descriptors:
 *
 * <ul>
 *   <li>Unbounded: {@code (Ljava/util/Collection;)Ljava/util/Map;} —
 *       beginningOffsets(Collection)</li>
 *   <li>Bounded:
 *       {@code (Ljava/util/Collection;Ljava/time/Duration;)Ljava/util/Map;}
 *       — beginningOffsets(Collection, Duration)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the no-Duration descriptor; the
 * bounded overload has a strictly different signature and is never
 * flagged.
 */
public final class ConsumerBeginningOffsetsNoTimeoutRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "beginningOffsets";
    private static final String NO_TIMEOUT_DESC = "(Ljava/util/Collection;)Ljava/util/Map;";

    private final Severity severity;

    public ConsumerBeginningOffsetsNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_BEGINNING_OFFSETS_NO_TIMEOUT;
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
                RuleId.CONSUMER_BEGINNING_OFFSETS_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.beginningOffsets(Collection) (no Duration) is "
                        + "reached here — either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::beginningOffsets` bound to a "
                        + "Function or Callable SAM, common shapes when "
                        + "retention/SLA monitors or replay-from-earliest "
                        + "tools dispatch offset reads via "
                        + "ScheduledExecutorService). The no-Duration "
                        + "overload is documented as equivalent to "
                        + "beginningOffsets(partitions, Duration.ofMillis("
                        + "defaultApiTimeoutMs)) where "
                        + "default.api.timeout.ms defaults to 60 s — the "
                        + "ListOffsets request to the partition leader is "
                        + "retried until that budget expires, with no "
                        + "shorter caller-side bound available. Under "
                        + "leader unavailability (leader election in "
                        + "progress, broker rolling restart, ISR "
                        + "shrinking, client/leader network partition), "
                        + "leader slowness (disk pressure on the segment "
                        + "storing the earliest offset — especially after "
                        + "a log-retention cycle just rolled the earliest "
                        + "segment, OS-level page-cache eviction, broker "
                        + "GC pause), or stale-metadata retargeting "
                        + "(cached metadata points at an old leader; "
                        + "NotLeaderOrFollowerException triggers a "
                        + "metadata refresh and a retry against the new "
                        + "leader under the same 60 s budget), the call "
                        + "blocks for the full default.api.timeout.ms. "
                        + "Five concrete failure modes follow: (1) "
                        + "retention / log-truncation monitors pin "
                        + "threads during the exact incidents they exist "
                        + "to detect — a compliance / SLA monitor polls "
                        + "`consumer.beginningOffsets(assignment)` every "
                        + "N seconds and alerts when the earliest offset "
                        + "advances past a configured threshold "
                        + "(indicating log-retention is deleting "
                        + "un-replayed data); under a broker outage the "
                        + "call blocks for the full 60 s, subsequent "
                        + "ticks queue behind it, the monitor goes silent "
                        + "during the exact retention-storm incident it "
                        + "exists to detect; (2) replay-from-earliest "
                        + "tools appear 'stuck' during exactly the "
                        + "incidents that prompt replay — common operator "
                        + "workflow is `beginningOffsets` to discover the "
                        + "earliest available offset (so the seek does "
                        + "not target an offset that retention has "
                        + "already deleted), then `seek` + replay; under "
                        + "broker slowness the first `beginningOffsets` "
                        + "blocks for 60 s and the operator cannot "
                        + "distinguish 'tool is hung' from 'cluster is "
                        + "slow', slowing incident response; (3) "
                        + "health-probe blocking pins kubelet "
                        + "liveness/readiness paths — an HTTP readiness "
                        + "endpoint that calls `beginningOffsets` to "
                        + "confirm broker reachability blocks the handler "
                        + "thread for 60 s under broker outage; the "
                        + "kubelet probe timeout (typically 1-3 s) fires "
                        + "and the Pod is restarted, masking the "
                        + "root-cause outage behind a thrashing restart "
                        + "loop; (4) scheduler-thread pinning when "
                        + "`beginningOffsets` is dispatched via "
                        + "ScheduledExecutorService method-reference "
                        + "capture — a Callable-typed indy capture "
                        + "`executor.submit((Callable) "
                        + "consumer::beginningOffsets)` pins the executor "
                        + "worker thread for 60 s on broker outage; "
                        + "backlogged tasks (heartbeats, metric emission, "
                        + "health probes) queue behind it; the executor's "
                        + "queue saturates and tasks are silently "
                        + "dropped; (5) INVOKEDYNAMIC "
                        + "`consumer::beginningOffsets` captures bypass "
                        + "naïve MethodInsnNode-only lint — a Function or "
                        + "Callable parameter bound by a "
                        + "`consumer::beginningOffsets` method reference "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeVirtual or "
                        + "REF_invokeInterface handle pointing at "
                        + "Consumer.beginningOffsets(Collection); the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEVIRTUAL on the no-Duration overload, "
                        + "only the indy site. Migration: use the bounded "
                        + "overload `beginningOffsets(Collection, "
                        + "Duration)` matched to the surrounding deadline "
                        + "(probe interval, monitor tick interval, "
                        + "scheduler task budget) and let the "
                        + "TimeoutException surface a broker-outage error "
                        + "rather than an indefinite-feeling 60 s hang. "
                        + "The bounded overload has descriptor "
                        + "`(Ljava/util/Collection;Ljava/time/Duration;)"
                        + "Ljava/util/Map;` and is never flagged by this "
                        + "rule.");
    }
}
