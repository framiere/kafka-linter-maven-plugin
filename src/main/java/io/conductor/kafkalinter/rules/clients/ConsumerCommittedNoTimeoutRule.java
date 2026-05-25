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
 * {@link org.apache.kafka.clients.consumer.Consumer#committed(java.util.Set)}
 * overload — descriptor {@code (Ljava/util/Set;)Ljava/util/Map;} with
 * no Duration argument. Catches both direct
 * {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE} calls and indirect
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code consumer::committed} bound to a
 * {@link java.util.function.Function} or a custom SAM) via a dual
 * walk over each method's instructions.
 *
 * <h2>Why no-Duration committed is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#committed(java.util.Set)}
 * is documented as equivalent to {@code committed(partitions,
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s. The call sends an
 * OffsetFetch request to the group coordinator asking for the last
 * committed offset of every supplied partition, and parks the caller
 * until a response arrives or the budget expires. The group
 * coordinator can be:
 *
 * <ul>
 *   <li><b>Unavailable</b> — coordinator unreachable during partition
 *       rebalance, leader election in progress on
 *       {@code __consumer_offsets}, broker rolling restart, ISR
 *       shrinking, network partition. The Consumer's coordinator-
 *       discovery and retry logic retries until
 *       {@code default.api.timeout.ms} expires; only then does a
 *       {@link org.apache.kafka.common.errors.TimeoutException}
 *       surface.</li>
 *   <li><b>Slow</b> — saturation by concurrent OffsetFetch traffic
 *       from a large consumer group, GC pause, disk pressure on
 *       {@code __consumer_offsets}. The full 60 s budget is consumed
 *       per call.</li>
 *   <li><b>Coordinator-moved</b> — the cached coordinator metadata
 *       points at an old broker; the request fails with
 *       {@code NotCoordinatorException}, coordinator metadata is
 *       refreshed and the request is retried, all under the same
 *       60 s budget.</li>
 * </ul>
 *
 * <p>Critically, lag monitoring is one of the most common consumers
 * of this overload — and the operational dynamic is paradoxical: the
 * tool that's supposed to alert on coordinator incidents pins its
 * own threads during coordinator incidents and stops emitting fresh
 * lag samples right when those samples matter most.
 *
 * <p>Five concrete failure modes (carried verbatim into the violation
 * message so the engineer reading the lint report understands the
 * "why" without leaving the IDE):
 *
 * <ul>
 *   <li><b>lag monitors freeze during exactly the coordinator
 *       incidents that justify having a lag monitor.</b> Pattern: a
 *       {@code @Scheduled(fixedDelay = 30_000)} job calls
 *       {@code consumer.committed(assignment)} every 30 s, diffs
 *       against {@code endOffsets} (also no-Duration), and emits a
 *       {@code consumer_lag} metric. Under coordinator outage each
 *       tick takes 60 s; ticks overlap; the scheduler pool fills;
 *       the {@code consumer_lag} metric goes stale; the dashboard
 *       wired to the alert shows "lag = (unknown, last value 5 min
 *       ago)" and the alert never fires because the threshold is
 *       computed on freshly arrived samples.</li>
 *   <li><b>HTTP admin endpoints that expose 'committed offset per
 *       partition' starve their handler thread pool.</b> Pattern: a
 *       REST endpoint exposes {@code consumer.committed(
 *       Set.of(partitionsFromQuery))} for ad-hoc operator queries;
 *       under coordinator slowness each request blocks for 60 s; the
 *       Jetty/Tomcat handler pool fills up; new requests are
 *       rejected with 503; the operator dashboard that motivated
 *       the endpoint goes blank.</li>
 *   <li><b>graceful shutdown overshoots terminationGracePeriodSeconds
 *       when called from a finally block.</b> A {@code finally}
 *       block calls {@code committed} to record the final committed
 *       offsets to a sidecar store before {@code close}; under
 *       coordinator slowness the call eats the entire 60 s grace
 *       period; kubelet sends SIGKILL; the sidecar store is not
 *       updated.</li>
 *   <li><b>scheduler-thread pinning when {@code committed} is
 *       dispatched via {@link java.util.concurrent.ScheduledExecutorService}
 *       method-reference capture.</b> An indy capture
 *       {@code executor.submit(() -> consumer.committed(partitions))}
 *       or {@code consumer::committed} bound to a custom SAM pins the
 *       executor worker thread for 60 s on coordinator outage;
 *       backlogged tasks queue behind it; the executor's queue
 *       saturates and tasks are silently dropped.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::committed} captures bypass
 *       naïve MethodInsnNode-only lint.</b> A
 *       {@link java.util.function.Function}{@code <Set<
 *       TopicPartition>, Map<TopicPartition, OffsetAndMetadata>>} or
 *       custom {@code @FunctionalInterface CommittedReader{
 *       Map<TopicPartition, OffsetAndMetadata> read(Set<
 *       TopicPartition> ps);}} parameter bound by a
 *       {@code consumer::committed} method reference compiles to
 *       {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} (KafkaConsumer typed receiver) or
 *       {@code REF_invokeInterface} (Consumer interface typed
 *       receiver) handle pointing at {@code Consumer.committed(
 *       Set)Ljava/util/Map;}. The user-class bytecode contains zero
 *       direct {@code INVOKEVIRTUAL} on the no-Duration overload —
 *       only the indy site. A rule that walks only
 *       {@code MethodInsnNode} misses every such site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — one no-Duration overload, one
 * bounded overload</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares two
 * committed overloads with different descriptors:
 *
 * <ul>
 *   <li>Unbounded: {@code (Ljava/util/Set;)Ljava/util/Map;} —
 *       committed(Set)</li>
 *   <li>Bounded:
 *       {@code (Ljava/util/Set;Ljava/time/Duration;)Ljava/util/Map;}
 *       — committed(Set, Duration)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the no-Duration descriptor;
 * the bounded overload has a strictly different signature and is
 * never flagged.
 */
public final class ConsumerCommittedNoTimeoutRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "committed";
    private static final String NO_TIMEOUT_DESC = "(Ljava/util/Set;)Ljava/util/Map;";

    private final Severity severity;

    public ConsumerCommittedNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_COMMITTED_NO_TIMEOUT;
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
                RuleId.CONSUMER_COMMITTED_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.committed(Set) (no Duration) is reached here — "
                        + "either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::committed` bound to a Function or a "
                        + "custom SAM, common shapes when lag monitors, "
                        + "HTTP admin endpoints, or graceful-shutdown "
                        + "handlers dispatch committed-offset reads via "
                        + "ScheduledExecutorService). The no-Duration "
                        + "overload is documented as equivalent to "
                        + "committed(partitions, Duration.ofMillis("
                        + "defaultApiTimeoutMs)) where "
                        + "default.api.timeout.ms defaults to 60 s — the "
                        + "call sends an OffsetFetch request to the group "
                        + "coordinator asking for the last committed "
                        + "offset of every supplied partition, and parks "
                        + "the caller until a response arrives or the "
                        + "budget expires. Under coordinator "
                        + "unavailability (coordinator unreachable during "
                        + "partition rebalance, leader election in "
                        + "progress on __consumer_offsets, broker rolling "
                        + "restart, ISR shrinking, network partition; the "
                        + "Consumer's coordinator-discovery and retry "
                        + "logic retries until the 60 s budget expires), "
                        + "coordinator slowness (saturation by concurrent "
                        + "OffsetFetch traffic from a large consumer "
                        + "group, GC pause, disk pressure on "
                        + "__consumer_offsets), or coordinator-moved "
                        + "(NotCoordinatorException triggers coordinator "
                        + "rediscovery and a retry under the same 60 s "
                        + "budget), the call blocks for the full "
                        + "default.api.timeout.ms. Lag monitoring is one "
                        + "of the most common consumers of this overload "
                        + "and the operational dynamic is paradoxical: "
                        + "the tool that's supposed to alert on "
                        + "coordinator incidents pins its own threads "
                        + "during coordinator incidents and stops "
                        + "emitting fresh lag samples right when those "
                        + "samples matter most. Five concrete failure "
                        + "modes follow: (1) lag monitors freeze during "
                        + "exactly the coordinator incidents that justify "
                        + "having a lag monitor — a "
                        + "`@Scheduled(fixedDelay = 30_000)` job calls "
                        + "`consumer.committed(assignment)` every 30 s, "
                        + "diffs against endOffsets (also no-Duration), "
                        + "and emits a `consumer_lag` metric; under "
                        + "coordinator outage each tick takes 60 s, "
                        + "ticks overlap, the scheduler pool fills, the "
                        + "`consumer_lag` metric goes stale, the "
                        + "dashboard wired to the alert shows 'lag = "
                        + "(unknown, last value 5 min ago)' and the "
                        + "alert never fires because the threshold is "
                        + "computed on freshly arrived samples; (2) HTTP "
                        + "admin endpoints that expose 'committed offset "
                        + "per partition' starve their handler thread "
                        + "pool — a REST endpoint exposes "
                        + "`consumer.committed(Set.of("
                        + "partitionsFromQuery))` for ad-hoc operator "
                        + "queries; under coordinator slowness each "
                        + "request blocks for 60 s; the Jetty/Tomcat "
                        + "handler pool fills up; new requests are "
                        + "rejected with 503; the operator dashboard "
                        + "that motivated the endpoint goes blank; (3) "
                        + "graceful shutdown overshoots "
                        + "terminationGracePeriodSeconds when called "
                        + "from a finally block — a `finally` block "
                        + "calls `committed` to record the final "
                        + "committed offsets to a sidecar store before "
                        + "`close`; under coordinator slowness the call "
                        + "eats the entire 60 s grace period; kubelet "
                        + "sends SIGKILL; the sidecar store is not "
                        + "updated; (4) scheduler-thread pinning when "
                        + "`committed` is dispatched via "
                        + "ScheduledExecutorService method-reference "
                        + "capture — an indy capture "
                        + "`executor.submit(() -> consumer.committed("
                        + "partitions))` or `consumer::committed` bound "
                        + "to a custom SAM pins the executor worker "
                        + "thread for 60 s on coordinator outage; "
                        + "backlogged tasks queue behind it; the "
                        + "executor's queue saturates and tasks are "
                        + "silently dropped; (5) INVOKEDYNAMIC "
                        + "`consumer::committed` captures bypass naïve "
                        + "MethodInsnNode-only lint — a "
                        + "Function<Set<TopicPartition>, "
                        + "Map<TopicPartition, OffsetAndMetadata>> or "
                        + "custom `@FunctionalInterface CommittedReader{"
                        + "Map<TopicPartition, OffsetAndMetadata> "
                        + "read(Set<TopicPartition> ps);}` parameter "
                        + "bound by a `consumer::committed` method "
                        + "reference compiles to INVOKEDYNAMIC whose "
                        + "bsm-args contain a REF_invokeVirtual or "
                        + "REF_invokeInterface handle pointing at "
                        + "Consumer.committed(Set)Ljava/util/Map;; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEVIRTUAL on the no-Duration overload, "
                        + "only the indy site. Migration: use the "
                        + "bounded overload `committed(Set, Duration)` "
                        + "matched to the surrounding deadline (lag-tick "
                        + "scheduler budget, HTTP request deadline, "
                        + "terminationGracePeriodSeconds minus a buffer) "
                        + "and let the TimeoutException surface a "
                        + "coordinator-outage error rather than an "
                        + "indefinite-feeling 60 s hang — the lag "
                        + "monitor then emits a sentinel 'coordinator "
                        + "unreachable' value that the alerting rule can "
                        + "specifically detect, instead of going silent. "
                        + "The bounded overload has descriptor "
                        + "`(Ljava/util/Set;Ljava/time/Duration;)"
                        + "Ljava/util/Map;` and is never flagged by "
                        + "this rule.");
    }
}
