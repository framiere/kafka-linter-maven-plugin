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
 * Fires for every reach of the no-reason
 * {@link org.apache.kafka.clients.consumer.Consumer#enforceRebalance()}
 * overload — descriptor {@code ()V} with no {@code String reason}
 * argument. Catches both direct {@code INVOKEVIRTUAL}/{@code
 * INVOKEINTERFACE} calls and indirect {@code INVOKEDYNAMIC} method-
 * reference captures (e.g. {@code consumer::enforceRebalance} bound
 * to {@link Runnable} or a custom no-arg SAM, common shapes when the
 * rebalance trigger is dispatched via
 * {@link java.util.concurrent.ScheduledExecutorService} or a Spring
 * {@code @Scheduled} method reference).
 *
 * <h2>Why no-reason enforceRebalance is an observability hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#enforceRebalance()}
 * is the application-side trigger for a group rebalance. KIP-735
 * (Kafka 3.0+) added the {@code enforceRebalance(String reason)}
 * overload precisely because operators investigating "why did this
 * group rebalance N times in M minutes?" were left with no
 * cross-actor attribution: the no-reason overload sends an
 * OffsetCommit request that is followed by a join-group cycle on the
 * coordinator, the coordinator-side state shows only "client X
 * requested rebalance", and the SRE has to correlate against
 * application logs, deploy timelines, and autoscaler signals to
 * guess who triggered it.
 *
 * <p>The reason String is recorded on the broker-side group-
 * coordinator log and made visible via {@code kafka-consumer-groups
 * --describe --members --verbose} and downstream telemetry. With a
 * reason every rebalance becomes self-describing — "autoscaler scale-
 * out svc-payments-prod 21→30", "operator manual rebalance, MTR-
 * 4831", "follower-fetch latency spike triggered partition-rebalance
 * after circuit-breaker"; without a reason every rebalance becomes a
 * forensic puzzle.
 *
 * <p>Concrete failure modes (carried into the violation message):
 *
 * <ul>
 *   <li><b>Rebalance-storm root-cause is unattributable.</b> Three
 *       independent actors call {@code consumer.enforceRebalance()}
 *       in the same process (autoscaler, partition-reassigner sidecar,
 *       config-refresher); the group rebalances 50× in 15 minutes;
 *       partitions are toxic for half that window. The
 *       coordinator-side log captures the same "rebalance triggered
 *       by client X" line for every call; the SRE cannot tell which
 *       of the three actors caused which rebalance. With a reason
 *       String every line attributes to the actor that called it.</li>
 *   <li><b>Operator-vs-application rebalance distinction is lost.</b>
 *       Two sources of rebalance: operator-side
 *       {@code kafka-consumer-groups --reset-offsets} (broker emits
 *       reason "manual reset") and application-side
 *       {@code consumer.enforceRebalance()} (no reason). With the
 *       String reason the SRE can grep the broker log for "manual
 *       reset" vs the application's reason tag; without it both
 *       sources look identical.</li>
 *   <li><b>Cross-service correlation is missing.</b> A control-plane
 *       service triggers rebalance on a downstream consumer group via
 *       an admin RPC; the downstream consumer calls
 *       {@code enforceRebalance()}; the control-plane request-id is
 *       NOT recorded. With a reason string of the shape
 *       {@code "control-plane req=abc123 svc=foo"} the SRE can join
 *       the broker-side rebalance event with the upstream
 *       control-plane request in their distributed-trace store.</li>
 *   <li><b>Spurious-rebalance detection on retired pods is blind.</b>
 *       A Spring {@code @PreDestroy} hook calls
 *       {@code enforceRebalance()} on shutdown to expedite partition
 *       handoff; without a reason the broker cannot distinguish this
 *       graceful event from a panic-mode trigger. With a reason like
 *       "graceful shutdown pod=X" the broker-side rebalance-counter
 *       can be filtered down to actually-unexpected events.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::enforceRebalance} captures
 *       bypass naïve MethodInsnNode-only lint.</b> A {@link Runnable}
 *       parameter (the SAM {@code void run()} erases to {@code ()V})
 *       bound by a {@code consumer::enforceRebalance} method
 *       reference compiles to {@code INVOKEDYNAMIC} whose bsm-args
 *       contain a {@code REF_invokeVirtual} or
 *       {@code REF_invokeInterface} handle pointing at
 *       {@code Consumer.enforceRebalance()V}. The user-class
 *       bytecode contains zero direct {@code INVOKEVIRTUAL} on the
 *       no-reason overload — only the indy site. Especially common
 *       in
 *       {@code scheduler.scheduleAtFixedRate(consumer::enforceRebalance,
 *       0, 5, MINUTES)} for periodic rebalance-trigger sidecars.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares two
 * enforceRebalance overloads:
 *
 * <ul>
 *   <li>Unsafe: {@code ()V} — enforceRebalance()</li>
 *   <li>Safe: {@code (Ljava/lang/String;)V} —
 *       enforceRebalance(String reason)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the no-reason descriptor; the
 * reason-carrying overload has a strictly different signature and is
 * never flagged.
 */
public final class ConsumerEnforceRebalanceNoReasonRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "enforceRebalance";
    private static final String NO_REASON_DESC = "()V";

    private final Severity severity;

    public ConsumerEnforceRebalanceNoReasonRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_ENFORCE_REBALANCE_NO_REASON;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && NO_REASON_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, NO_REASON_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.CONSUMER_ENFORCE_REBALANCE_NO_REASON, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.enforceRebalance() (no reason String) is "
                        + "reached here — either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::enforceRebalance` bound to a "
                        + "Runnable or no-arg custom SAM, common when "
                        + "the trigger is dispatched via "
                        + "ScheduledExecutorService — "
                        + "`scheduler.scheduleAtFixedRate("
                        + "consumer::enforceRebalance, 0, 5, MINUTES)` "
                        + "for periodic rebalance-trigger sidecars). "
                        + "KIP-735 (Kafka 3.0+) added the "
                        + "enforceRebalance(String reason) overload so "
                        + "broker-side group-coordinator logs capture "
                        + "WHO triggered each manually-initiated "
                        + "rebalance — investigation of "
                        + "'why did this group rebalance N times in M "
                        + "minutes?' needs cross-actor attribution. "
                        + "Concrete failure modes: (1) rebalance-storm "
                        + "root-cause is unattributable — three "
                        + "independent actors call enforceRebalance() "
                        + "in the same process (autoscaler, partition-"
                        + "reassigner sidecar, config-refresher); group "
                        + "rebalances 50x in 15 minutes; partitions are "
                        + "toxic for half that window; the broker log "
                        + "captures the same line for every call; the "
                        + "SRE cannot tell which actor caused which "
                        + "rebalance; with a reason string every line "
                        + "attributes to the actor that called it; (2) "
                        + "operator-vs-application rebalance "
                        + "distinction is lost — two sources of "
                        + "rebalance (operator-side `kafka-consumer-"
                        + "groups --reset-offsets`, application-side "
                        + "enforceRebalance()); with a reason String "
                        + "the SRE can distinguish them; without it "
                        + "both look identical in broker logs; (3) "
                        + "cross-service correlation is missing — "
                        + "control-plane service triggers rebalance on "
                        + "a downstream consumer via an admin RPC; "
                        + "without a reason the control-plane "
                        + "request-id is not recorded; with a reason "
                        + "like 'control-plane req=abc123 svc=foo' the "
                        + "SRE can join the broker-side rebalance event "
                        + "with the upstream control-plane request in "
                        + "the distributed-trace store; (4) spurious-"
                        + "rebalance detection on retired pods is "
                        + "blind — a Spring @PreDestroy hook calls "
                        + "enforceRebalance() on shutdown to expedite "
                        + "partition handoff; without a reason the "
                        + "broker cannot distinguish this graceful event "
                        + "from a panic-mode trigger; with a reason like "
                        + "'graceful shutdown pod=X' the broker-side "
                        + "rebalance-counter can be filtered down to "
                        + "actually-unexpected events; (5) INVOKEDYNAMIC "
                        + "`consumer::enforceRebalance` captures bypass "
                        + "naive MethodInsnNode-only lint — Runnable SAM "
                        + "`void run()` erases to `()V` bound by a "
                        + "`consumer::enforceRebalance` method reference "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeVirtual or "
                        + "REF_invokeInterface handle pointing at "
                        + "Consumer.enforceRebalance()V; the user-class "
                        + "bytecode contains zero direct INVOKEVIRTUAL "
                        + "on the no-reason overload, only the indy "
                        + "site; very common in "
                        + "`scheduler.scheduleAtFixedRate("
                        + "consumer::enforceRebalance, 0, 5, MINUTES)` "
                        + "for periodic rebalance-trigger sidecars. "
                        + "Migration: pass enforceRebalance(\"<actor> "
                        + "<event-context>\") — e.g. "
                        + "`enforceRebalance(\"autoscaler scale-out "
                        + "svc-payments-prod 21→30\")`, "
                        + "`enforceRebalance(\"operator manual "
                        + "rebalance MTR-4831\")`, "
                        + "`enforceRebalance(\"graceful shutdown pod=\" "
                        + "+ podName)`. The reason-carrying overload "
                        + "has descriptor `(Ljava/lang/String;)V` and "
                        + "is never flagged by this rule.");
    }
}
