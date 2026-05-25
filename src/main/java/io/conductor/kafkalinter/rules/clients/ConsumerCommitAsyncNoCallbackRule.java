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
 * Fires for every reach of the no-callback
 * {@link org.apache.kafka.clients.consumer.Consumer#commitAsync()}
 * overload — descriptor {@code ()V} with no
 * {@link org.apache.kafka.clients.consumer.OffsetCommitCallback}
 * argument. Catches both direct
 * {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE} calls and indirect
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code consumer::commitAsync} bound to {@link Runnable} or a
 * custom no-arg SAM) via a dual walk over each method's
 * instructions.
 *
 * <h2>Why no-callback commitAsync is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#commitAsync()}
 * is fire-and-forget by design: the method dispatches an
 * OffsetCommit request to the group coordinator and returns
 * immediately, before the broker has acknowledged the commit. There
 * are then three possible outcomes:
 *
 * <ul>
 *   <li><b>Commit succeeds</b> — the OffsetCommit response carries
 *       a success code, the in-memory committed-offset state is
 *       updated, the next OffsetFetch will see the new value.</li>
 *   <li><b>Commit fails with a retriable error</b> —
 *       coordinator unreachable
 *       ({@code COORDINATOR_NOT_AVAILABLE},
 *       {@code COORDINATOR_LOAD_IN_PROGRESS},
 *       {@code NOT_COORDINATOR}), rebalance in progress
 *       ({@code REBALANCE_IN_PROGRESS}), broker network blip.
 *       The Consumer does NOT auto-retry async commits; the
 *       response is delivered to the callback (if any) and
 *       discarded.</li>
 *   <li><b>Commit fails with a fatal error</b> — generation
 *       mismatch ({@code ILLEGAL_GENERATION}, member kicked out of
 *       group), authorization revoked
 *       ({@code TOPIC_AUTHORIZATION_FAILED},
 *       {@code GROUP_AUTHORIZATION_FAILED}). Same story: surfaced
 *       only via the callback.</li>
 * </ul>
 *
 * <p>The no-callback overload has no path to surface any of these
 * — the commit either silently succeeds or silently fails, and the
 * application has no signal whatsoever to detect, retry, alert,
 * or even log the failure. This is a far more dangerous default
 * than it appears: the consumer continues to process records,
 * accumulates progress in memory, and at every commit boundary
 * "tries" to commit. Eventually the process restarts (planned
 * deploy, OOM, node failure) and the consumer rejoins the group
 * with a committed offset from minutes-to-hours ago — every record
 * since the last successful commit is replayed, and any downstream
 * write that is not idempotent produces duplicates.
 *
 * <p>Five concrete failure modes (carried verbatim into the
 * violation message so the engineer reading the lint report
 * understands the "why" without leaving the IDE):
 *
 * <ul>
 *   <li><b>silent commit-loss during coordinator failover produces
 *       duplicate processing on restart.</b> Pattern: the consumer
 *       loop calls {@code consumer.commitAsync()} at the end of
 *       every batch. The coordinator fails over (rolling restart,
 *       network partition, leader election on
 *       {@code __consumer_offsets}). For the duration of the
 *       failover every async commit returns a retriable error
 *       (typically {@code NOT_COORDINATOR}), which is silently
 *       dropped because there is no callback. The consumer
 *       continues, processes thousands of records, and is restarted
 *       (deploy, OOM, eviction). On rejoin the committed offset is
 *       the last successful commit before the failover; every
 *       record since is replayed; downstream non-idempotent writes
 *       produce duplicates.</li>
 *   <li><b>silent commit-loss during a rebalance produces
 *       duplicate processing on the next-owner consumer.</b>
 *       Pattern: rebalance starts; ongoing async commits return
 *       {@code REBALANCE_IN_PROGRESS}; without a callback the
 *       application has no signal that the commits were rejected;
 *       the partition is reassigned to a different consumer
 *       instance; that instance starts from the last successful
 *       commit before the rebalance and re-processes records the
 *       previous owner had already handled.</li>
 *   <li><b>silent generation-mismatch loss after a session-
 *       timeout-driven group-kick.</b> Pattern: the consumer is
 *       slow on a long-running batch (GC pause, downstream
 *       backpressure), misses
 *       {@code session.timeout.ms}, is kicked from the group
 *       ({@code ILLEGAL_GENERATION}). The application doesn't know
 *       and keeps calling {@code commitAsync()}; every commit fails
 *       with {@code ILLEGAL_GENERATION}; nothing is logged; the
 *       application looks healthy in metrics while making zero
 *       committed progress until the next rebalance recovery.</li>
 *   <li><b>silent ACL-revocation loss during a credential
 *       rotation.</b> Pattern: an operator rotates the consumer's
 *       SASL principal or ACL; the {@code OffsetCommit} responses
 *       start coming back with {@code GROUP_AUTHORIZATION_FAILED}
 *       or {@code TOPIC_AUTHORIZATION_FAILED}; without a callback
 *       these never surface to the application; metrics show
 *       "healthy" (poll continues, processing continues) while
 *       offsets stop advancing — silent stuck commit. The bug is
 *       discovered hours later when a restart reveals a huge
 *       backlog of replayed work.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::commitAsync} captures
 *       bypass naïve MethodInsnNode-only lint.</b> A
 *       {@link Runnable} parameter (the SAM {@code void run()}
 *       erases to {@code ()V}) bound by a
 *       {@code consumer::commitAsync} method reference compiles to
 *       {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} (KafkaConsumer typed receiver) or
 *       {@code REF_invokeInterface} (Consumer interface typed
 *       receiver) handle pointing at {@code Consumer.commitAsync(
 *       )V}. The user-class bytecode contains zero direct
 *       {@code INVOKEVIRTUAL} on the no-callback overload — only
 *       the indy site. A rule that walks only
 *       {@code MethodInsnNode} misses every such site. This shape
 *       is very common when the commit is dispatched via
 *       {@link java.util.concurrent.ScheduledExecutorService}:
 *       {@code scheduler.scheduleAtFixedRate(consumer::commitAsync,
 *       0, 5, SECONDS)} — and the inability to detect commit
 *       failure is then amplified by the scheduler context (no log
 *       sink, no exception path, no test coverage).</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — three overloads, two callback-
 * carrying overloads are safe</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares
 * three commitAsync overloads:
 *
 * <ul>
 *   <li>Unsafe: {@code ()V} — commitAsync()</li>
 *   <li>Safe:
 *       {@code (Lorg/apache/kafka/clients/consumer/OffsetCommitCallback;)V}
 *       — commitAsync(OffsetCommitCallback)</li>
 *   <li>Safe:
 *       {@code (Ljava/util/Map;Lorg/apache/kafka/clients/consumer/OffsetCommitCallback;)V}
 *       — commitAsync(Map, OffsetCommitCallback)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the no-callback descriptor;
 * the two callback-carrying overloads have strictly different
 * signatures and are never flagged.
 */
public final class ConsumerCommitAsyncNoCallbackRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "commitAsync";
    private static final String NO_CALLBACK_DESC = "()V";

    private final Severity severity;

    public ConsumerCommitAsyncNoCallbackRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_COMMIT_ASYNC_NO_CALLBACK;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && NO_CALLBACK_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, NO_CALLBACK_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.CONSUMER_COMMIT_ASYNC_NO_CALLBACK, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.commitAsync() (no callback) is reached here — "
                        + "either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::commitAsync` bound to a Runnable or "
                        + "no-arg custom SAM, common when the commit is "
                        + "dispatched via ScheduledExecutorService — "
                        + "`scheduler.scheduleAtFixedRate("
                        + "consumer::commitAsync, 0, 5, SECONDS)`). "
                        + "commitAsync is fire-and-forget by design: the "
                        + "method dispatches an OffsetCommit request to "
                        + "the group coordinator and returns immediately, "
                        + "before the broker has acknowledged the commit. "
                        + "The Consumer does NOT auto-retry async "
                        + "commits — retriable errors (coordinator "
                        + "unreachable: COORDINATOR_NOT_AVAILABLE, "
                        + "COORDINATOR_LOAD_IN_PROGRESS, NOT_COORDINATOR; "
                        + "rebalance in progress: REBALANCE_IN_PROGRESS; "
                        + "broker network blip) and fatal errors "
                        + "(generation mismatch: ILLEGAL_GENERATION, "
                        + "member kicked out of group; authorization "
                        + "revoked: TOPIC_AUTHORIZATION_FAILED, "
                        + "GROUP_AUTHORIZATION_FAILED) are delivered to "
                        + "the callback (if any) and discarded. The "
                        + "no-callback overload has no path to surface "
                        + "any of these — the commit either silently "
                        + "succeeds or silently fails, and the "
                        + "application has no signal whatsoever to "
                        + "detect, retry, alert, or even log the "
                        + "failure. This is a far more dangerous default "
                        + "than it appears: the consumer continues to "
                        + "process records, accumulates progress in "
                        + "memory, and at every commit boundary 'tries' "
                        + "to commit. Eventually the process restarts "
                        + "(planned deploy, OOM, node failure) and the "
                        + "consumer rejoins the group with a committed "
                        + "offset from minutes-to-hours ago — every "
                        + "record since the last successful commit is "
                        + "replayed, and any downstream write that is "
                        + "not idempotent produces duplicates. Five "
                        + "concrete failure modes follow: (1) silent "
                        + "commit-loss during coordinator failover "
                        + "produces duplicate processing on restart — "
                        + "consumer loop calls "
                        + "`consumer.commitAsync()` at the end of every "
                        + "batch; coordinator fails over (rolling "
                        + "restart, network partition, leader election "
                        + "on __consumer_offsets); for the duration of "
                        + "the failover every async commit returns a "
                        + "retriable error (typically NOT_COORDINATOR), "
                        + "silently dropped; consumer continues, "
                        + "processes thousands of records, restarted "
                        + "(deploy, OOM, eviction); on rejoin the "
                        + "committed offset is the last successful "
                        + "commit before the failover, every record "
                        + "since is replayed, downstream non-idempotent "
                        + "writes produce duplicates; (2) silent "
                        + "commit-loss during a rebalance produces "
                        + "duplicate processing on the next-owner "
                        + "consumer — rebalance starts, ongoing async "
                        + "commits return REBALANCE_IN_PROGRESS, no "
                        + "signal that the commits were rejected, the "
                        + "partition is reassigned to a different "
                        + "consumer instance, that instance starts from "
                        + "the last successful commit before the "
                        + "rebalance and re-processes records the "
                        + "previous owner had already handled; (3) "
                        + "silent generation-mismatch loss after a "
                        + "session-timeout-driven group-kick — consumer "
                        + "slow on long-running batch (GC pause, "
                        + "downstream backpressure), misses "
                        + "session.timeout.ms, kicked from the group "
                        + "(ILLEGAL_GENERATION); application doesn't "
                        + "know, keeps calling `commitAsync()`; every "
                        + "commit fails with ILLEGAL_GENERATION; nothing "
                        + "is logged; application looks healthy in "
                        + "metrics while making zero committed progress "
                        + "until the next rebalance recovery; (4) "
                        + "silent ACL-revocation loss during a "
                        + "credential rotation — operator rotates the "
                        + "consumer's SASL principal or ACL, "
                        + "OffsetCommit responses come back with "
                        + "GROUP_AUTHORIZATION_FAILED or "
                        + "TOPIC_AUTHORIZATION_FAILED; without a "
                        + "callback these never surface; metrics show "
                        + "'healthy' (poll continues, processing "
                        + "continues) while offsets stop advancing — "
                        + "silent stuck commit; the bug is discovered "
                        + "hours later when a restart reveals a huge "
                        + "backlog of replayed work; (5) INVOKEDYNAMIC "
                        + "`consumer::commitAsync` captures bypass "
                        + "naïve MethodInsnNode-only lint — a Runnable "
                        + "parameter (SAM `void run()` erases to "
                        + "`()V`) bound by a `consumer::commitAsync` "
                        + "method reference compiles to INVOKEDYNAMIC "
                        + "whose bsm-args contain a REF_invokeVirtual or "
                        + "REF_invokeInterface handle pointing at "
                        + "Consumer.commitAsync()V; the user-class "
                        + "bytecode contains zero direct INVOKEVIRTUAL "
                        + "on the no-callback overload, only the indy "
                        + "site; very common when the commit is "
                        + "dispatched via ScheduledExecutorService — "
                        + "the inability to detect commit failure is "
                        + "then amplified by the scheduler context (no "
                        + "log sink, no exception path, no test "
                        + "coverage). Migration: use the bounded "
                        + "overload `commitAsync(OffsetCommitCallback)` "
                        + "or `commitAsync(Map<TopicPartition, "
                        + "OffsetAndMetadata>, OffsetCommitCallback)` "
                        + "with a callback that (a) logs the exception, "
                        + "(b) increments a `commit_async_failures` "
                        + "counter so failures show up on dashboards, "
                        + "(c) on retriable errors during a known "
                        + "incident, optionally falls back to a "
                        + "bounded `commitSync(Duration)` at the next "
                        + "natural batch boundary. The two "
                        + "callback-carrying overloads have descriptors "
                        + "`(Lorg/apache/kafka/clients/consumer/"
                        + "OffsetCommitCallback;)V` and "
                        + "`(Ljava/util/Map;Lorg/apache/kafka/clients/"
                        + "consumer/OffsetCommitCallback;)V` and are "
                        + "never flagged by this rule.");
    }
}
