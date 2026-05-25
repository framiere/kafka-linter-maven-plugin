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
 * {@link org.apache.kafka.clients.consumer.Consumer#endOffsets(java.util.Collection)}
 * overload — descriptor {@code (Ljava/util/Collection;)Ljava/util/Map;}
 * with no Duration argument. Catches both direct
 * {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE} calls and indirect
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code consumer::endOffsets} bound to a {@link java.util.function.Function}
 * or {@link java.util.concurrent.Callable}) via a dual walk over each
 * method's instructions.
 *
 * <h2>Why no-Duration endOffsets is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#endOffsets(java.util.Collection)}
 * is documented as "equivalent to endOffsets(partitions,
 * Duration.ofMillis(defaultApiTimeoutMs))" where
 * {@code default.api.timeout.ms} defaults to 60 s. The call sends a
 * ListOffsets request to the leader of each partition asking for the
 * latest offset, with no caller-side deadline tighter than 60 s. The
 * leader can be:
 *
 * <ul>
 *   <li><b>Unavailable</b> — leader election in progress, broker
 *       rolling restart, ISR shrinking, network partition between the
 *       client and the leader. The Consumer's metadata-refresh logic
 *       retries until {@code default.api.timeout.ms} expires; only
 *       then does a {@link org.apache.kafka.common.errors.TimeoutException}
 *       surface, with no shorter caller-side bound available.</li>
 *   <li><b>Slow</b> — disk pressure on the segment storing the latest
 *       offset, OS-level page-cache eviction, broker GC pause. The
 *       full 60 s budget is consumed per call.</li>
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
 *   <li><b>lag-monitoring scripts pin threads during the exact
 *       outages they exist to detect.</b> Pattern: a monitoring agent
 *       polls {@code consumer.endOffsets(assignment)} every N seconds,
 *       diffs the result against the last committed offsets, and
 *       publishes the gauge to Prometheus. Under a broker outage the
 *       call blocks for the full 60 s; subsequent ticks queue behind
 *       it; the gauge goes stale and the on-call team sees flat-lined
 *       lag during the incident that motivated the monitor.</li>
 *   <li><b>admin / replay tooling appears 'stuck' during exactly the
 *       incidents that prompt replay.</b> A common operator workflow
 *       is: discover an issue, call {@code endOffsets} to bound the
 *       replay window, then {@code seek} + replay. Under broker
 *       slowness the first {@code endOffsets} call blocks for 60 s
 *       and the operator cannot distinguish "tool is hung" from
 *       "cluster is slow" — incident response slows down.</li>
 *   <li><b>health-probe blocking pins kubelet liveness/readiness
 *       paths.</b> An HTTP readiness endpoint that calls
 *       {@code endOffsets} to confirm broker reachability blocks the
 *       handler thread for 60 s under broker outage; the kubelet
 *       probe timeout (typically 1-3 s) fires and the Pod is
 *       restarted, masking the root-cause outage behind a thrashing
 *       restart loop.</li>
 *   <li><b>scheduler-thread pinning when {@code endOffsets} is
 *       called via {@link java.util.concurrent.ScheduledExecutorService}
 *       method-reference dispatch.</b> A Callable-typed indy capture
 *       {@code executor.submit((Callable<Map<TopicPartition, Long>>)
 *       consumer::endOffsets)} pins the executor worker thread for
 *       60 s on broker outage; backlogged tasks (heartbeats, metric
 *       emission, health probes) queue behind it; the executor's
 *       queue saturates and tasks are silently dropped.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::endOffsets} captures bypass
 *       naïve MethodInsnNode-only lint.</b> A {@link java.util.function.Function}
 *       or {@link java.util.concurrent.Callable} parameter bound by a
 *       {@code consumer::endOffsets} method reference compiles to
 *       {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} (KafkaConsumer typed receiver) or
 *       {@code REF_invokeInterface} (Consumer interface typed
 *       receiver) handle pointing at {@code Consumer.endOffsets(
 *       Collection)}. The user-class bytecode contains zero direct
 *       {@code INVOKEVIRTUAL} on the no-Duration overload — only the
 *       indy site. A rule that walks only {@code MethodInsnNode}
 *       misses every such site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — one no-Duration overload, one
 * bounded overload</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares two
 * endOffsets overloads with different descriptors:
 *
 * <ul>
 *   <li>Unbounded: {@code (Ljava/util/Collection;)Ljava/util/Map;} —
 *       endOffsets(Collection)</li>
 *   <li>Bounded: {@code (Ljava/util/Collection;Ljava/time/Duration;)Ljava/util/Map;}
 *       — endOffsets(Collection, Duration)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the no-Duration descriptor; the
 * bounded overload has a strictly different signature and is never
 * flagged.
 */
public final class ConsumerEndOffsetsNoTimeoutRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "endOffsets";
    private static final String NO_TIMEOUT_DESC = "(Ljava/util/Collection;)Ljava/util/Map;";

    private final Severity severity;

    public ConsumerEndOffsetsNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_END_OFFSETS_NO_TIMEOUT;
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
                RuleId.CONSUMER_END_OFFSETS_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.endOffsets(Collection) (no Duration) is "
                        + "reached here — either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::endOffsets` bound to a Function or "
                        + "Callable SAM, common shapes when lag-monitor "
                        + "agents dispatch offset reads via "
                        + "ScheduledExecutorService). The no-Duration "
                        + "overload is documented as equivalent to "
                        + "endOffsets(partitions, Duration.ofMillis("
                        + "defaultApiTimeoutMs)) where "
                        + "default.api.timeout.ms defaults to 60 s — the "
                        + "ListOffsets request to the partition leader is "
                        + "retried until that budget expires, with no "
                        + "shorter caller-side bound available. Under "
                        + "leader unavailability (leader election in "
                        + "progress, broker rolling restart, ISR "
                        + "shrinking, client/leader network partition), "
                        + "leader slowness (disk pressure on the segment "
                        + "storing the latest offset, OS-level page-cache "
                        + "eviction, broker GC pause), or stale-metadata "
                        + "retargeting (cached metadata points at an old "
                        + "leader; NotLeaderOrFollowerException triggers "
                        + "a metadata refresh and a retry against the new "
                        + "leader under the same 60 s budget), the call "
                        + "blocks for the full default.api.timeout.ms. "
                        + "Five concrete failure modes follow: (1) "
                        + "lag-monitoring scripts pin threads during the "
                        + "exact outages they exist to detect — a "
                        + "monitoring agent polls "
                        + "`consumer.endOffsets(assignment)` every N "
                        + "seconds, diffs the result against last "
                        + "committed offsets, and publishes the gauge; "
                        + "under a broker outage the call blocks for the "
                        + "full 60 s, subsequent ticks queue behind it, "
                        + "the gauge goes stale and the on-call team sees "
                        + "flat-lined lag during the incident that "
                        + "motivated the monitor; (2) admin / replay "
                        + "tooling appears 'stuck' during exactly the "
                        + "incidents that prompt replay — common operator "
                        + "workflow is `endOffsets` to bound the replay "
                        + "window, then `seek` + replay; under broker "
                        + "slowness the first `endOffsets` blocks for "
                        + "60 s and the operator cannot distinguish 'tool "
                        + "is hung' from 'cluster is slow', slowing "
                        + "incident response; (3) health-probe blocking "
                        + "pins kubelet liveness/readiness paths — an "
                        + "HTTP readiness endpoint that calls "
                        + "`endOffsets` to confirm broker reachability "
                        + "blocks the handler thread for 60 s under "
                        + "broker outage; the kubelet probe timeout "
                        + "(typically 1-3 s) fires and the Pod is "
                        + "restarted, masking the root-cause outage "
                        + "behind a thrashing restart loop; (4) "
                        + "scheduler-thread pinning when `endOffsets` is "
                        + "called via ScheduledExecutorService "
                        + "method-reference dispatch — a Callable-typed "
                        + "indy capture "
                        + "`executor.submit((Callable<Map<TopicPartition,"
                        + " Long>>) consumer::endOffsets)` pins the "
                        + "executor worker thread for 60 s on broker "
                        + "outage; backlogged tasks (heartbeats, metric "
                        + "emission, health probes) queue behind it; the "
                        + "executor's queue saturates and tasks are "
                        + "silently dropped; (5) INVOKEDYNAMIC "
                        + "`consumer::endOffsets` captures bypass naïve "
                        + "MethodInsnNode-only lint — a Function or "
                        + "Callable parameter bound by a "
                        + "`consumer::endOffsets` method reference "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeVirtual or "
                        + "REF_invokeInterface handle pointing at "
                        + "Consumer.endOffsets(Collection); the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEVIRTUAL on the no-Duration overload, "
                        + "only the indy site. Migration: use the bounded "
                        + "overload `endOffsets(Collection, Duration)` "
                        + "matched to the surrounding deadline (probe "
                        + "interval, monitor tick interval, scheduler "
                        + "task budget) and let the TimeoutException "
                        + "surface a broker-outage error rather than an "
                        + "indefinite-feeling 60 s hang. The bounded "
                        + "overload has descriptor "
                        + "`(Ljava/util/Collection;Ljava/time/Duration;)"
                        + "Ljava/util/Map;` and is never flagged by this "
                        + "rule.");
    }
}
