package io.conductor.kafkalinter.rules.clients;

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
 * Fires for every reach of an unbounded
 * {@link org.apache.kafka.clients.consumer.Consumer#commitSync()} or
 * {@link org.apache.kafka.clients.consumer.Consumer#commitSync(java.util.Map)}
 * overload — the descriptors {@code ()V} and {@code (Ljava/util/Map;)V}
 * with no Duration argument.
 *
 * <p>The rule catches both direct
 * {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE} calls and indirect
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code consumer::commitSync} bound to {@link Runnable}) via a dual
 * walk over each method's instructions.
 *
 * <h2>Why no-Duration commitSync is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#commitSync()}
 * is documented as "equivalent to commitSync(Duration.ofMillis(
 * Long.MAX_VALUE))" — it has no caller-side deadline. The call sends
 * an OffsetCommit request to the group coordinator and parks the
 * calling thread until the response arrives. The coordinator can be:
 *
 * <ul>
 *   <li><b>Unavailable</b> — partition leader rebalance, network
 *       partition, broker rolling restart, controller election in
 *       progress. The Consumer's metadata-refresh logic retries
 *       forever; commitSync never returns until the coordinator
 *       becomes reachable again.</li>
 *   <li><b>Slow</b> — saturated by concurrent commit traffic from a
 *       large consumer group, GC pause, disk pressure on
 *       __consumer_offsets. The wait grows unboundedly with no
 *       caller-side timeout.</li>
 *   <li><b>Lost in a coordinator move</b> — group coordinator
 *       migrates to a different broker; the existing in-flight
 *       commitSync waits for the original coordinator until metadata
 *       is refreshed and a Retriable retry path is taken — but the
 *       Consumer's internal retry budget is the only bound.</li>
 * </ul>
 *
 * <p>Five concrete failure modes (carried verbatim into the violation
 * message so the engineer reading the lint report understands the
 * "why" without leaving the IDE):
 *
 * <ul>
 *   <li><b>poll-loop pinning.</b> A typical at-least-once consumer
 *       loop calls {@code consumer.commitSync()} after each batch.
 *       Under a slow coordinator, the loop stalls — no new poll() is
 *       issued, the consumer's session.timeout.ms expires, the group
 *       coordinator considers the member dead, partitions are
 *       rebalanced away, and on coordinator recovery the original
 *       caller eventually returns but its partition assignment is
 *       gone. The next poll() throws CommitFailedException for the
 *       batch we just blocked trying to commit.</li>
 *   <li><b>graceful-shutdown hooks hang forever.</b> A common
 *       shutdown sequence is {@code wakeup()} the poll loop, drain
 *       in-flight work, {@code commitSync()} the final offsets, then
 *       {@code close()}. The final commitSync is exactly the call
 *       the application cannot afford to hang on — it runs while the
 *       JVM is responding to SIGTERM, with a hard Pod
 *       terminationGracePeriodSeconds budget. Under a slow
 *       coordinator the commitSync overshoots the grace period,
 *       kubelet sends SIGKILL, the offsets are not committed, and
 *       the next instance reprocesses the un-committed batch
 *       (duplicate side effects).</li>
 *   <li><b>scheduled / cron offset checkpointing pins the
 *       scheduler thread.</b> Pattern: a {@code ScheduledExecutor}
 *       runs {@code consumer::commitSync} every N seconds to flush
 *       offsets out-of-band of the poll loop. Under a slow
 *       coordinator the scheduler's thread is pinned indefinitely;
 *       subsequent scheduled tasks (heartbeats, metric emission,
 *       health probes) queue behind it; eventually the executor's
 *       queue saturates and tasks are silently dropped.</li>
 *   <li><b>per-partition commitSync(Map) leaks the same hazard
 *       with finer granularity.</b> A consumer that commits only a
 *       subset of assigned partitions via the
 *       {@code commitSync(Map<TopicPartition, OffsetAndMetadata>)}
 *       overload exposes the same unbounded blocking semantics on
 *       descriptor {@code (Ljava/util/Map;)V}. The smaller commit
 *       payload does not bound the wait — both descriptors are
 *       flagged by this rule.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::commitSync} captures
 *       bypass naïve MethodInsnNode-only lint.</b> A
 *       {@link Runnable} parameter bound by a
 *       {@code consumer::commitSync} method reference (e.g.
 *       {@code scheduledExecutor.scheduleAtFixedRate(consumer::commitSync,
 *       0, 30, SECONDS)}) compiles to {@code INVOKEDYNAMIC} whose
 *       bsm-args contain a {@code REF_invokeVirtual} or
 *       {@code REF_invokeInterface} handle pointing at
 *       {@code Consumer.commitSync()V}. The user-class bytecode
 *       contains zero direct {@code INVOKEVIRTUAL} on the no-arg
 *       commitSync — only the indy site. A rule that walks only
 *       {@code MethodInsnNode} misses every such site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — two no-Duration overloads, two
 * bounded overloads</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares four
 * commitSync overloads with different descriptors:
 *
 * <ul>
 *   <li>Unbounded: {@code ()V} — commitSync()</li>
 *   <li>Unbounded: {@code (Ljava/util/Map;)V} — commitSync(Map)</li>
 *   <li>Bounded: {@code (Ljava/time/Duration;)V} —
 *       commitSync(Duration)</li>
 *   <li>Bounded: {@code (Ljava/util/Map;Ljava/time/Duration;)V} —
 *       commitSync(Map, Duration)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the two no-Duration descriptors;
 * the bounded overloads have strictly different signatures and are
 * never flagged.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code consumer::commitSync} bound to a SAM whose erased
 * descriptor matches {@code ()V} (e.g. {@link Runnable}) compiles to
 * {@code INVOKEDYNAMIC} whose bsm-args contain a method-handle
 * reference (kind depends on whether the user code references
 * {@code KafkaConsumer} the class or {@code Consumer} the interface).
 * The rule walks every indy's bsm-args and matches owners × name ×
 * descriptor against {@code Consumer × commitSync × (()V or
 * (Ljava/util/Map;)V)}.
 *
 * <p>{@code consumer::commitSync} bound to a SAM matching
 * {@code (Ljava/util/Map;)V} is realistic when a custom helper
 * abstracts per-partition commit flushing; the same indy walk catches
 * that shape too.
 */
public final class ConsumerCommitSyncNoTimeoutRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "commitSync";
    private static final Set<String> NO_TIMEOUT_DESCS = Set.of(
            "()V",
            "(Ljava/util/Map;)V");

    private final Severity severity;

    public ConsumerCommitSyncNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_COMMITSYNC_NO_TIMEOUT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && NO_TIMEOUT_DESCS.contains(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    for (String desc : NO_TIMEOUT_DESCS) {
                        Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, desc);
                        if (h != null) {
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
                RuleId.CONSUMER_COMMITSYNC_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.commitSync() / commitSync(Map) (no Duration) "
                        + "is reached here — either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::commitSync` bound to a Runnable SAM, "
                        + "the universal shape when scheduling out-of-band "
                        + "offset flushes via ScheduledExecutorService). The "
                        + "no-Duration overload is documented as "
                        + "equivalent to commitSync(Duration.ofMillis("
                        + "Long.MAX_VALUE)) — it sends an OffsetCommit "
                        + "request to the group coordinator and parks the "
                        + "calling thread until the response arrives, with "
                        + "no caller-side deadline. Under coordinator "
                        + "unavailability (partition leader rebalance, "
                        + "network partition, broker rolling restart, "
                        + "controller election in progress), slowness "
                        + "(saturation by concurrent commit traffic, GC "
                        + "pause, disk pressure on __consumer_offsets), or "
                        + "a coordinator move (group coordinator migrates "
                        + "to a different broker), the call hangs "
                        + "indefinitely. Five concrete failure modes "
                        + "follow: (1) poll-loop pinning — a typical "
                        + "at-least-once consumer loop calls "
                        + "`consumer.commitSync()` after each batch; under "
                        + "a slow coordinator, the loop stalls — no new "
                        + "poll() is issued, the consumer's "
                        + "session.timeout.ms expires, the group "
                        + "coordinator considers the member dead, "
                        + "partitions are rebalanced away, and on "
                        + "coordinator recovery the original caller "
                        + "eventually returns but its partition assignment "
                        + "is gone; the next poll() throws "
                        + "CommitFailedException for the batch we just "
                        + "blocked trying to commit; (2) graceful-shutdown "
                        + "hooks hang forever — a common shutdown sequence "
                        + "is `wakeup()` the poll loop, drain in-flight "
                        + "work, `commitSync()` the final offsets, then "
                        + "`close()`; the final commitSync is exactly the "
                        + "call the application cannot afford to hang on — "
                        + "it runs while the JVM is responding to SIGTERM "
                        + "with a hard Pod terminationGracePeriodSeconds "
                        + "budget; under a slow coordinator the commitSync "
                        + "overshoots the grace period, kubelet sends "
                        + "SIGKILL, the offsets are not committed, and the "
                        + "next instance reprocesses the un-committed batch "
                        + "(duplicate side effects); (3) scheduled / cron "
                        + "offset checkpointing pins the scheduler thread "
                        + "— pattern: a ScheduledExecutor runs "
                        + "`consumer::commitSync` every N seconds to flush "
                        + "offsets out-of-band of the poll loop; under a "
                        + "slow coordinator the scheduler's thread is "
                        + "pinned indefinitely; subsequent scheduled tasks "
                        + "(heartbeats, metric emission, health probes) "
                        + "queue behind it; eventually the executor's queue "
                        + "saturates and tasks are silently dropped; (4) "
                        + "per-partition commitSync(Map) leaks the same "
                        + "hazard with finer granularity — a consumer that "
                        + "commits only a subset of assigned partitions via "
                        + "the commitSync(Map<TopicPartition, "
                        + "OffsetAndMetadata>) overload exposes the same "
                        + "unbounded blocking semantics on descriptor "
                        + "(Ljava/util/Map;)V; the smaller commit payload "
                        + "does not bound the wait — both descriptors are "
                        + "flagged by this rule; (5) INVOKEDYNAMIC "
                        + "`consumer::commitSync` captures bypass naïve "
                        + "MethodInsnNode-only lint — a Runnable parameter "
                        + "bound by a `consumer::commitSync` method "
                        + "reference (e.g. "
                        + "`scheduledExecutor.scheduleAtFixedRate("
                        + "consumer::commitSync, 0, 30, SECONDS)`) compiles "
                        + "to INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeVirtual or REF_invokeInterface handle "
                        + "pointing at Consumer.commitSync()V; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEVIRTUAL on the no-arg commitSync, only the "
                        + "indy site. Migration: use the bounded overload "
                        + "`commitSync(Duration)` or "
                        + "`commitSync(Map, Duration)` matched to the "
                        + "surrounding deadline (poll budget, "
                        + "terminationGracePeriodSeconds minus a buffer, "
                        + "scheduler tick interval) and let the "
                        + "TimeoutException surface a coordinator-outage "
                        + "error rather than an indefinite hang. The "
                        + "bounded overloads have descriptors "
                        + "`(Ljava/time/Duration;)V` and "
                        + "`(Ljava/util/Map;Ljava/time/Duration;)V` and are "
                        + "never flagged by this rule.");
    }
}
