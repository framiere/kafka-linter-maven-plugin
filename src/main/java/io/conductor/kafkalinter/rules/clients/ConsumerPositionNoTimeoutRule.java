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
 * {@link org.apache.kafka.clients.consumer.Consumer#position(org.apache.kafka.common.TopicPartition)}
 * overload — descriptor
 * {@code (Lorg/apache/kafka/common/TopicPartition;)J} with no Duration
 * argument. Catches both direct
 * {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE} calls and indirect
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code consumer::position} bound to a custom SAM or
 * {@link java.util.function.ToLongFunction}) via a dual walk over each
 * method's instructions.
 *
 * <h2>Why no-Duration position is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#position(org.apache.kafka.common.TopicPartition)}
 * is documented as equivalent to {@code position(partition,
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s. When the consumer
 * holds a cached position the call returns immediately; when the
 * cached position is stale (just after a rebalance, after a seek to a
 * timestamp that needs server resolution, after an
 * {@code OFFSET_OUT_OF_RANGE} reset), the call sends an
 * OffsetFetch / ListOffsets / coordinator request and parks the
 * caller until a response arrives. The coordinator / leader can be:
 *
 * <ul>
 *   <li><b>Unavailable</b> — group coordinator unreachable during
 *       partition rebalance, leader election in progress, broker
 *       rolling restart, ISR shrinking, network partition. The
 *       Consumer's metadata-refresh logic retries until
 *       {@code default.api.timeout.ms} expires; only then does a
 *       {@link org.apache.kafka.common.errors.TimeoutException}
 *       surface.</li>
 *   <li><b>Slow</b> — saturation by concurrent OffsetFetch traffic
 *       from a large consumer group, GC pause, disk pressure on
 *       __consumer_offsets. The full 60 s budget is consumed per
 *       call.</li>
 *   <li><b>Stale-metadata-targeted</b> — the cached metadata points
 *       at an old leader; the request fails with
 *       {@code NotLeaderOrFollowerException}, metadata is refreshed
 *       and retried under the same 60 s budget.</li>
 * </ul>
 *
 * <p>The hazard is multiplied per partition because callers
 * commonly loop {@code for (TopicPartition tp : assignment)
 * consumer.position(tp);} — a 12-partition assignment can therefore
 * hang for 12 × 60 s = 12 minutes under sustained coordinator
 * unavailability before the first TimeoutException surfaces.
 *
 * <p>Five concrete failure modes (carried verbatim into the violation
 * message so the engineer reading the lint report understands the
 * "why" without leaving the IDE):
 *
 * <ul>
 *   <li><b>per-partition lag probes pin threads for N × 60 s under
 *       coordinator outage.</b> Pattern: lag monitor loops over the
 *       consumer's assignment, calls {@code consumer.position(tp)}
 *       per partition, diffs against committed offsets. With 12
 *       assigned partitions and a stale coordinator the loop hangs
 *       for up to 12 minutes before any TimeoutException
 *       surfaces.</li>
 *   <li><b>graceful shutdown overshoots terminationGracePeriodSeconds
 *       when called from a finally block.</b> A {@code finally}
 *       block calls {@code position} to record final offsets to a
 *       sidecar store before {@code close}. Under coordinator
 *       slowness the position call eats the entire 60 s grace
 *       period; kubelet sends SIGKILL; the sidecar store is not
 *       updated; the next instance starts from a stale offset
 *       baseline.</li>
 *   <li><b>HTTP admin endpoints that expose 'current position per
 *       partition' starve their handler thread pool.</b> A REST
 *       endpoint exposes {@code consumer.position(tp)} for
 *       monitoring; under broker slowness each request blocks for
 *       60 s; the Jetty/Tomcat handler pool fills up; new requests
 *       are rejected with 503. The dashboard that motivated the
 *       endpoint goes blank during the exact incident it exists to
 *       observe.</li>
 *   <li><b>scheduler-thread pinning when {@code position} is
 *       dispatched via {@link java.util.concurrent.ScheduledExecutorService}
 *       method-reference capture.</b> An indy capture
 *       {@code executor.submit(() -> consumer.position(tp))} or
 *       {@code consumer::position} bound to a custom SAM pins the
 *       executor worker thread for 60 s on coordinator outage;
 *       backlogged tasks queue behind it; the executor's queue
 *       saturates and tasks are silently dropped.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::position} captures bypass
 *       naïve MethodInsnNode-only lint.</b> A custom
 *       {@code @FunctionalInterface PositionReader{long get(
 *       TopicPartition tp);}} or
 *       {@link java.util.function.ToLongFunction} parameter bound by
 *       a {@code consumer::position} method reference compiles to
 *       {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} (KafkaConsumer typed receiver) or
 *       {@code REF_invokeInterface} (Consumer interface typed
 *       receiver) handle pointing at {@code Consumer.position(
 *       TopicPartition)J}. The user-class bytecode contains zero
 *       direct {@code INVOKEVIRTUAL} on the no-Duration overload —
 *       only the indy site. A rule that walks only
 *       {@code MethodInsnNode} misses every such site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — one no-Duration overload, one
 * bounded overload</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares two
 * position overloads with different descriptors:
 *
 * <ul>
 *   <li>Unbounded:
 *       {@code (Lorg/apache/kafka/common/TopicPartition;)J} —
 *       position(TopicPartition)</li>
 *   <li>Bounded:
 *       {@code (Lorg/apache/kafka/common/TopicPartition;Ljava/time/Duration;)J}
 *       — position(TopicPartition, Duration)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the no-Duration descriptor;
 * the bounded overload has a strictly different signature and is
 * never flagged.
 */
public final class ConsumerPositionNoTimeoutRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "position";
    private static final String NO_TIMEOUT_DESC = "(Lorg/apache/kafka/common/TopicPartition;)J";

    private final Severity severity;

    public ConsumerPositionNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_POSITION_NO_TIMEOUT;
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
                RuleId.CONSUMER_POSITION_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.position(TopicPartition) (no Duration) is "
                        + "reached here — either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::position` bound to a custom SAM or "
                        + "ToLongFunction, common shapes when "
                        + "per-partition lag probes or HTTP admin "
                        + "endpoints dispatch position reads via "
                        + "ScheduledExecutorService). The no-Duration "
                        + "overload is documented as equivalent to "
                        + "position(partition, Duration.ofMillis("
                        + "defaultApiTimeoutMs)) where "
                        + "default.api.timeout.ms defaults to 60 s — "
                        + "when the cached position is stale (just after "
                        + "a rebalance, after a seek to a timestamp that "
                        + "needs server resolution, after an "
                        + "OFFSET_OUT_OF_RANGE reset), the call sends an "
                        + "OffsetFetch / ListOffsets / coordinator "
                        + "request and parks the caller until a response "
                        + "arrives. Under coordinator unavailability "
                        + "(coordinator unreachable during partition "
                        + "rebalance, leader election in progress, broker "
                        + "rolling restart, ISR shrinking, network "
                        + "partition), slowness (saturation by "
                        + "concurrent OffsetFetch traffic from a large "
                        + "consumer group, GC pause, disk pressure on "
                        + "__consumer_offsets), or stale-metadata "
                        + "retargeting (NotLeaderOrFollowerException "
                        + "triggers metadata refresh + retry under the "
                        + "same 60 s budget), the call blocks for the "
                        + "full default.api.timeout.ms. The hazard is "
                        + "multiplied per partition because callers "
                        + "commonly loop `for (TopicPartition tp : "
                        + "assignment) consumer.position(tp);` — a "
                        + "12-partition assignment can therefore hang "
                        + "for 12 × 60 s = 12 minutes under sustained "
                        + "coordinator unavailability before the first "
                        + "TimeoutException surfaces. Five concrete "
                        + "failure modes follow: (1) per-partition lag "
                        + "probes pin threads for N × 60 s under "
                        + "coordinator outage — lag monitor loops over "
                        + "the consumer's assignment, calls "
                        + "`consumer.position(tp)` per partition, diffs "
                        + "against committed offsets; with 12 assigned "
                        + "partitions and a stale coordinator the loop "
                        + "hangs for up to 12 minutes before any "
                        + "TimeoutException surfaces; (2) graceful "
                        + "shutdown overshoots "
                        + "terminationGracePeriodSeconds when called "
                        + "from a finally block — a `finally` block "
                        + "calls `position` to record final offsets to "
                        + "a sidecar store before `close`; under "
                        + "coordinator slowness the position call eats "
                        + "the entire 60 s grace period; kubelet sends "
                        + "SIGKILL; the sidecar store is not updated; "
                        + "the next instance starts from a stale offset "
                        + "baseline; (3) HTTP admin endpoints that "
                        + "expose 'current position per partition' "
                        + "starve their handler thread pool — a REST "
                        + "endpoint exposes `consumer.position(tp)` for "
                        + "monitoring; under broker slowness each "
                        + "request blocks for 60 s; the Jetty/Tomcat "
                        + "handler pool fills up; new requests are "
                        + "rejected with 503; the dashboard that "
                        + "motivated the endpoint goes blank during the "
                        + "exact incident it exists to observe; (4) "
                        + "scheduler-thread pinning when `position` is "
                        + "dispatched via ScheduledExecutorService "
                        + "method-reference capture — an indy capture "
                        + "`executor.submit(() -> consumer.position(tp))"
                        + "` or `consumer::position` bound to a custom "
                        + "SAM pins the executor worker thread for 60 s "
                        + "on coordinator outage; backlogged tasks queue "
                        + "behind it; the executor's queue saturates "
                        + "and tasks are silently dropped; (5) "
                        + "INVOKEDYNAMIC `consumer::position` captures "
                        + "bypass naïve MethodInsnNode-only lint — a "
                        + "custom `@FunctionalInterface PositionReader{"
                        + "long get(TopicPartition tp);}` or "
                        + "ToLongFunction parameter bound by a "
                        + "`consumer::position` method reference "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeVirtual or "
                        + "REF_invokeInterface handle pointing at "
                        + "Consumer.position(TopicPartition)J; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEVIRTUAL on the no-Duration overload, "
                        + "only the indy site. Migration: use the "
                        + "bounded overload `position(TopicPartition, "
                        + "Duration)` matched to the surrounding "
                        + "deadline (HTTP request deadline, "
                        + "terminationGracePeriodSeconds minus a "
                        + "buffer, scheduler task budget) and let the "
                        + "TimeoutException surface a coordinator-outage "
                        + "error rather than an indefinite-feeling 60 s "
                        + "× N-partitions hang. The bounded overload has "
                        + "descriptor `(Lorg/apache/kafka/common/"
                        + "TopicPartition;Ljava/time/Duration;)J` and is "
                        + "never flagged by this rule.");
    }
}
