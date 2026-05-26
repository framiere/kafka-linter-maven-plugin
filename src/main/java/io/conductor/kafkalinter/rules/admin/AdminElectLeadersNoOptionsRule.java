package io.conductor.kafkalinter.rules.admin;

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
 * Fires for every reach of an {@link
 * org.apache.kafka.clients.admin.Admin#electLeaders
 * Admin.electLeaders} overload that does NOT include an {@link
 * org.apache.kafka.clients.admin.ElectLeadersOptions
 * ElectLeadersOptions} argument. The single unsafe overload is
 * the 2-arg {@code electLeaders(ElectionType, Set<
 * TopicPartition>)}. The 3-arg Options-bearing overload is the
 * safe form and is intentionally not flagged.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} on {@link
 * org.apache.kafka.clients.admin.Admin Admin}, {@code
 * INVOKEVIRTUAL} on {@link
 * org.apache.kafka.clients.admin.AdminClient AdminClient}, and
 * {@code INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-Options {@code electLeaders} is an availability /
 * data-loss hazard depending on the {@link
 * org.apache.kafka.common.ElectionType ElectionType}
 * argument</h2>
 *
 * <p>{@link
 * org.apache.kafka.clients.admin.Admin#electLeaders
 * Admin.electLeaders} triggers a controller-side leader
 * election against a chosen set of partitions:
 * <ul>
 *   <li>{@code ElectionType.PREFERRED} — re-elects the
 *       PREFERRED replica (first in the assignment list)
 *       as leader; safe in steady-state but can briefly
 *       interrupt producers/consumers as the leader epoch
 *       advances and clients re-fetch metadata.</li>
 *   <li>{@code ElectionType.UNCLEAN} — elects ANY in-sync OR
 *       OUT-OF-SYNC replica as leader; the canonical
 *       "data-loss-tolerant unstick" used when all ISR
 *       members are unavailable. UNCLEAN election PERMANENTLY
 *       drops the records that were on the previous leader
 *       but never replicated to the new leader — this is a
 *       data-loss event by design.</li>
 * </ul>
 *
 * <p>The no-Options overload inherits the default {@code
 * request.timeout.ms} (~30 s). The controller's response is
 * gated on {@code LeaderAndIsr} writes propagating to the
 * affected brokers — during exactly the kind of incident where
 * an operator reaches for {@code electLeaders} (e.g. a stuck
 * preferred-leader-imbalance after a broker bounce, or a
 * cluster-wide unclean recovery), the controller is precisely
 * the entity that is contended. The 30 s timeout fires; the
 * caller does not know whether the election was applied; a
 * naïve retry resubmits — and now the SAME partitions get
 * elected TWICE in quick succession, doubling the
 * client-visible leader-epoch advance and inflating the
 * metadata-refresh storm.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Cruise Control's preferred-leader-election job
 *       times out mid-batch and the cluster's leader skew
 *       persists.</b> Cruise Control's "topic-replica-skew"
 *       goal periodically calls {@code electLeaders(
 *       ElectionType.PREFERRED, partitions)} with a batch of
 *       300 partitions to undo leadership skew that has
 *       accumulated since the last broker restart. The
 *       controller is processing the resulting {@code
 *       LeaderAndIsr} responses; the 30 s timeout fires; the
 *       Cruise Control job marks the rebalance task FAILED;
 *       the next anomaly detector run sees the skew is still
 *       present and re-submits. Now the controller has TWO
 *       pending elect-leaders for the same partitions; the
 *       leader-epoch advances twice in seconds; every client
 *       sees TWO {@code NotLeaderOrFollowerException} bursts
 *       and re-fetches metadata twice; producer p99 latency
 *       spikes; tail-latency SLO breach.</li>
 *   <li><b>UNCLEAN election retry compounds data loss.</b> A
 *       runbook calls {@code electLeaders(ElectionType.
 *       UNCLEAN, Set.of(tp))} to recover a partition whose
 *       entire ISR is unreachable. The controller starts the
 *       election; it elects broker B (out of sync, missing
 *       the last 500 records that were on the failed leader);
 *       the response is queued behind other controller work;
 *       the 30 s timeout fires; the operator's script
 *       retries; the second {@code electLeaders} sees broker
 *       B is now the leader (success-fast); BUT the operator
 *       is reading the first attempt's exception and
 *       concludes the election failed — they look for a
 *       different broker to elect manually, e.g. via {@code
 *       kafka-leader-election.sh --topic foo --partition 0
 *       --election-type unclean --bootstrap-server …}. That
 *       second manual election against broker C triggers
 *       another truncation event because C's high-water-mark
 *       was different than B's; producers lose ANOTHER batch
 *       of records that B did manage to ingest in the seconds
 *       between elections.</li>
 *   <li><b>{@code null} partitions overload — broad blast
 *       radius without timeout.</b> Passing {@code null} or
 *       an empty set for the partitions argument elects ALL
 *       partitions cluster-wide. On a large cluster (tens of
 *       thousands of partitions) the controller takes well
 *       over 30 s to process the batch; the no-Options call
 *       fires {@code TimeoutException} long before the batch
 *       completes; the operator does NOT know which subset
 *       was elected; the leader-epoch storm propagates across
 *       the entire cluster for minutes; consumer-group
 *       rebalance storms follow because every consumer
 *       resets its position relative to the new
 *       leader-epoch.</li>
 *   <li><b>K8s operator (Strimzi, Confluent) marks
 *       KafkaTopic NotReady on transient elect-leaders
 *       timeouts.</b> A Strimzi reconciliation step uses
 *       {@code electLeaders(PREFERRED, partitions)} after a
 *       broker rolling restart to restore preferred
 *       leadership; the call times out during the busy
 *       post-restart metadata propagation window; the
 *       reconciliation marks the {@code KafkaTopic} as {@code
 *       NotReady}; downstream {@code Deployment} resources
 *       with {@code dependsOn} on the topic stay blocked
 *       through the operator's next reconciliation loop
 *       (minutes), turning a routine post-restart healing
 *       step into a deploy block.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       leader-election helper built as {@code BiFunction<
 *       ElectionType, Set<TopicPartition>, ElectLeadersResult
 *       > elect = admin::electLeaders} captures an {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle on the 2-arg overload.
 *       The user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Options overload, only
 *       the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.clients.admin.ElectLeadersOptions
 * ElectLeadersOptions} pinning a per-call deadline — e.g.
 * {@code admin.electLeaders(ElectionType.PREFERRED,
 * partitions, new ElectLeadersOptions().timeoutMs(120_000))}.
 * For {@code ElectionType.UNCLEAN} specifically, treat the
 * call as IRREVERSIBLE (any retry on timeout is a NEW
 * data-loss event) — capture per-partition {@code
 * result.partitions()} success/fail futures BEFORE
 * concluding the election failed, and NEVER retry an UNCLEAN
 * call on {@code TimeoutException} without first inspecting
 * cluster state.
 */
public final class AdminElectLeadersNoOptionsRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.ADMIN_OWNERS;
    private static final String METHOD_NAME = "electLeaders";
    private static final String OPTIONS_TYPE_TOKEN =
            "Lorg/apache/kafka/clients/admin/ElectLeadersOptions;";

    private final Severity severity;

    public AdminElectLeadersNoOptionsRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_ELECT_LEADERS_NO_OPTIONS;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && mi.desc != null
                        && !mi.desc.contains(OPTIONS_TYPE_TOKEN)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, null);
                    if (h != null && h.getDesc() != null && !h.getDesc().contains(OPTIONS_TYPE_TOKEN)) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.ADMIN_ELECT_LEADERS_NO_OPTIONS, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Admin.electLeaders(ElectionType, Set<TopicPartition>) "
                        + "— a no-ElectLeadersOptions overload is reached "
                        + "here, either as a direct INVOKEINTERFACE on "
                        + "Admin / INVOKEVIRTUAL on AdminClient or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`admin::electLeaders` bound to BiFunction<"
                        + "ElectionType, Set<TopicPartition>, "
                        + "ElectLeadersResult> or to a custom SAM whose "
                        + "erased implMethod descriptor matches the "
                        + "unsafe overload). electLeaders triggers a "
                        + "controller-side leader election against a "
                        + "chosen set of partitions: PREFERRED re-elects "
                        + "the PREFERRED replica (first in the assignment "
                        + "list) as leader (safe in steady-state but can "
                        + "briefly interrupt producers/consumers as the "
                        + "leader epoch advances); UNCLEAN elects ANY "
                        + "in-sync OR OUT-OF-SYNC replica as leader (the "
                        + "canonical data-loss-tolerant unstick used "
                        + "when all ISR members are unavailable; UNCLEAN "
                        + "election PERMANENTLY drops the records that "
                        + "were on the previous leader but never "
                        + "replicated to the new leader — a data-loss "
                        + "event by design). The no-Options overload "
                        + "inherits the default request.timeout.ms "
                        + "(~30s). The controller's response is gated "
                        + "on LeaderAndIsr writes propagating to the "
                        + "affected brokers — during exactly the kind "
                        + "of incident where an operator reaches for "
                        + "electLeaders (a stuck "
                        + "preferred-leader-imbalance after a broker "
                        + "bounce, or a cluster-wide unclean recovery), "
                        + "the controller is precisely the entity that "
                        + "is contended; the 30s timeout fires; the "
                        + "caller does not know whether the election "
                        + "was applied; a naive retry resubmits — and "
                        + "now the SAME partitions get elected TWICE "
                        + "in quick succession, doubling the "
                        + "client-visible leader-epoch advance and "
                        + "inflating the metadata-refresh storm. "
                        + "Concrete failure modes: (1) Cruise Control's "
                        + "preferred-leader-election job times out "
                        + "mid-batch and the cluster's leader skew "
                        + "persists; Cruise Control's "
                        + "topic-replica-skew goal periodically calls "
                        + "electLeaders(ElectionType.PREFERRED, "
                        + "partitions) with a batch of 300 partitions; "
                        + "the controller is processing the resulting "
                        + "LeaderAndIsr responses; the 30s timeout "
                        + "fires; the Cruise Control job marks the "
                        + "rebalance task FAILED; the next anomaly "
                        + "detector run sees the skew is still present "
                        + "and re-submits; now the controller has TWO "
                        + "pending elect-leaders for the same "
                        + "partitions; the leader-epoch advances twice "
                        + "in seconds; every client sees TWO "
                        + "NotLeaderOrFollowerException bursts and "
                        + "re-fetches metadata twice; producer p99 "
                        + "latency spikes; tail-latency SLO breach; "
                        + "(2) UNCLEAN election retry compounds data "
                        + "loss; a runbook calls electLeaders("
                        + "ElectionType.UNCLEAN, Set.of(tp)) to "
                        + "recover a partition whose entire ISR is "
                        + "unreachable; the controller starts the "
                        + "election; it elects broker B (out of sync, "
                        + "missing the last 500 records that were on "
                        + "the failed leader); the response is queued "
                        + "behind other controller work; the 30s "
                        + "timeout fires; the operator's script "
                        + "retries; the second electLeaders sees "
                        + "broker B is now the leader (success-fast); "
                        + "BUT the operator is reading the first "
                        + "attempt's exception and concludes the "
                        + "election failed — they look for a different "
                        + "broker to elect manually via "
                        + "kafka-leader-election.sh; that second "
                        + "manual election against broker C triggers "
                        + "another truncation event because C's "
                        + "high-water-mark was different than B's; "
                        + "producers lose ANOTHER batch of records "
                        + "that B did manage to ingest in the seconds "
                        + "between elections; (3) null partitions "
                        + "overload — broad blast radius without "
                        + "timeout; passing null or an empty set for "
                        + "the partitions argument elects ALL "
                        + "partitions cluster-wide; on a large cluster "
                        + "(tens of thousands of partitions) the "
                        + "controller takes well over 30s to process "
                        + "the batch; the no-Options call fires "
                        + "TimeoutException long before the batch "
                        + "completes; the operator does NOT know which "
                        + "subset was elected; the leader-epoch storm "
                        + "propagates across the entire cluster for "
                        + "minutes; consumer-group rebalance storms "
                        + "follow because every consumer resets its "
                        + "position relative to the new leader-epoch; "
                        + "(4) K8s operator (Strimzi, Confluent) marks "
                        + "KafkaTopic NotReady on transient "
                        + "elect-leaders timeouts; a Strimzi "
                        + "reconciliation step uses electLeaders("
                        + "PREFERRED, partitions) after a broker "
                        + "rolling restart to restore preferred "
                        + "leadership; the call times out during the "
                        + "busy post-restart metadata propagation "
                        + "window; the reconciliation marks the "
                        + "KafkaTopic as NotReady; downstream "
                        + "Deployment resources with dependsOn on the "
                        + "topic stay blocked through the operator's "
                        + "next reconciliation loop (minutes), turning "
                        + "a routine post-restart healing step into a "
                        + "deploy block; (5) INVOKEDYNAMIC "
                        + "method-reference captures bypass naive "
                        + "MethodInsnNode-only lint — BiFunction<"
                        + "ElectionType, Set<TopicPartition>, "
                        + "ElectLeadersResult> elect = admin::"
                        + "electLeaders captures an INVOKEDYNAMIC "
                        + "whose bsm-args contain a "
                        + "REF_invokeInterface Handle on the 2-arg "
                        + "overload; the user-class bytecode contains "
                        + "zero direct INVOKEINTERFACE on the "
                        + "no-Options overload, only the indy site. "
                        + "Migration: pass an explicit "
                        + "ElectLeadersOptions pinning a per-call "
                        + "deadline — admin.electLeaders(ElectionType."
                        + "PREFERRED, partitions, new "
                        + "ElectLeadersOptions().timeoutMs(120_000)). "
                        + "For ElectionType.UNCLEAN specifically, "
                        + "treat the call as IRREVERSIBLE (any retry "
                        + "on timeout is a NEW data-loss event) — "
                        + "capture per-partition result.partitions() "
                        + "success/fail futures BEFORE concluding the "
                        + "election failed, and NEVER retry an "
                        + "UNCLEAN call on TimeoutException without "
                        + "first inspecting cluster state.");
    }
}
