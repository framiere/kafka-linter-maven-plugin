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
 * org.apache.kafka.clients.admin.Admin#alterPartitionReassignments
 * Admin.alterPartitionReassignments} overload that does NOT
 * include an {@link
 * org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions
 * AlterPartitionReassignmentsOptions} argument. The single unsafe
 * overload is the 1-arg {@code alterPartitionReassignments(Map<
 * TopicPartition, Optional<NewPartitionReassignment>>)}. The
 * 2-arg Options-bearing overload is the safe form and is
 * intentionally not flagged.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} on {@link
 * org.apache.kafka.clients.admin.Admin Admin}, {@code
 * INVOKEVIRTUAL} on {@link
 * org.apache.kafka.clients.admin.AdminClient AdminClient}, and
 * {@code INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-Options {@code alterPartitionReassignments} is a
 * production hazard for an IRREVERSIBLE controller-side
 * mutation</h2>
 *
 * <p>{@link
 * org.apache.kafka.clients.admin.Admin#alterPartitionReassignments
 * Admin.alterPartitionReassignments} is the KIP-455 write-side
 * RPC — it submits a partition reassignment plan to the
 * controller, which writes the {@code /admin/reassignments}
 * znode (KRaft: the {@code PartitionChangeRecord} batch),
 * triggers {@code LeaderAndIsr} requests to source/destination
 * brokers, and begins migrating partition replicas. Once the
 * controller has accepted the plan it is APPLIED — the only way
 * to "undo" a reassignment is to submit a new reassignment that
 * reverses it. There is no transactional rollback; there is no
 * idempotency token; the operation modifies cluster topology in
 * a way that is observable to every consumer and producer
 * within seconds.
 *
 * <p>The no-Options overload inherits the default {@code
 * request.timeout.ms} (~30 s) from the AdminClient
 * configuration. The CONTROLLER is the entity handling the
 * request, and the controller is exactly the entity that is
 * busiest during high-traffic periods (ISR shrink/expand for
 * lagging followers, broker session expiry, etc.) — those are
 * the same windows during which an SRE most often initiates a
 * reassignment ("the cluster is unhealthy; rebalance it"). When
 * the future fails with {@code TimeoutException} the SRE
 * tooling has NO way to distinguish "the controller rejected
 * the plan" from "the controller accepted the plan and is
 * applying it but the response was lost" from "the request
 * never reached the controller". A naïve retry-on-timeout
 * implementation re-submits the plan — and the SECOND submit
 * either no-ops (if the first was accepted) or COMPOUNDS the
 * change (if the user has constructed the second plan from
 * post-first-plan cluster state). Either way, the operator
 * loses observability over the cluster's true topology state.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Duplicate-submission on naïve retry — controller
 *       applies the plan twice, doubling the data movement.</b>
 *       An SRE script wraps {@code alterPartitionReassignments
 *       (plan)} in a "retry 3× on timeout" decorator. The
 *       controller is busy; the first submit succeeds
 *       controller-side but the response is lost at the 30 s
 *       boundary; the future completes exceptionally with
 *       {@code TimeoutException}. The retry decorator resubmits
 *       the same {@code plan} map; this time the call returns
 *       success. The user believes one reassignment was
 *       applied; in reality the controller saw the same plan
 *       twice. While the controller dedups identical plans for
 *       the same partition, it does NOT dedup overlapping plans
 *       — and if the SRE script regenerated the plan from
 *       live cluster state between attempts, the second plan
 *       references different replica sets and the controller
 *       applies BOTH, leading to two cascading rebalances and
 *       roughly 2× the inter-broker data movement.</li>
 *   <li><b>Atomicity loss — partial batch applied, caller sees
 *       failure.</b> A reassignment plan covers 200 partitions
 *       across 5 topics. The controller begins writing the
 *       {@code PartitionChangeRecord} batch; partway through
 *       (e.g. partition 137) it processes a queued {@code
 *       AlterIsr} request and the write is delayed. The 30 s
 *       timeout fires on the AdminClient side; the future
 *       completes exceptionally; the SRE script logs "FAILED"
 *       and aborts the deploy. Meanwhile the controller
 *       finishes the batch — partitions 0–136 have the new
 *       assignment, partitions 137–199 have the new assignment
 *       (KRaft writes the whole batch atomically, but the SRE
 *       script doesn't know that). The script now believes the
 *       plan failed; the next deploy step depends on
 *       "no reassignment is active"; it polls {@code
 *       listPartitionReassignments()}, sees 200 active
 *       reassignments, and exits with "unexpected cluster
 *       state, aborting."</li>
 *   <li><b>Reassignment-during-incident — caller is the
 *       healing-tool itself and timing out makes the incident
 *       worse.</b> Cruise Control's anomaly-detector triggers
 *       a self-healing reassignment when a broker becomes
 *       unhealthy. The {@code alterPartitionReassignments} call
 *       inherits the default 30 s timeout. The unhealthy broker
 *       is dragging down ISR membership, which is causing the
 *       controller to queue {@code AlterIsr} requests; the
 *       controller is contended; the self-healing submit times
 *       out. Cruise Control's anomaly-detector marks the
 *       healing action as failed and disables itself per the
 *       "halt on repeated failure" config — so the controller
 *       continues to shed brokers and the operator has just
 *       lost the automation that was supposed to mitigate the
 *       incident.</li>
 *   <li><b>Cancellation requires explicit empty-reassignment;
 *       a stuck call leaves the operator unable to reason about
 *       in-flight state.</b> The intended way to cancel an
 *       in-flight reassignment is to call {@code
 *       alterPartitionReassignments(Map.of(tp, Optional.empty()
 *       ))}. If the cancellation call inherits the 30 s
 *       timeout and the controller is busy processing the very
 *       reassignment the operator wants to cancel, the
 *       cancellation times out; the operator does not know
 *       whether the cancel was applied; they cannot safely
 *       submit a new plan because they do not know which
 *       partitions are still in motion; the cluster
 *       effectively stays in a frozen-rebalance state until
 *       human escalation re-establishes visibility.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       reassignment-submission helper built as {@code
 *       Function<Map<TopicPartition, Optional<
 *       NewPartitionReassignment>>,
 *       AlterPartitionReassignmentsResult> submit = admin::
 *       alterPartitionReassignments} captures an {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle on the 1-arg overload. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Options overload, only the
 *       indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions
 * AlterPartitionReassignmentsOptions} pinning a per-call
 * deadline that gives the controller enough time to durably
 * accept the plan even under load — e.g. {@code admin.
 * alterPartitionReassignments(plan, new
 * AlterPartitionReassignmentsOptions().timeoutMs(120_000))}.
 * For an irreversible mutation the right move is to pick a
 * timeout that is generous enough to almost always succeed
 * controller-side, plus an idempotency strategy (e.g.
 * "submit-then-poll-listPartitionReassignments-to-confirm-
 * acceptance" rather than "submit-then-retry-on-timeout").
 */
public final class AdminAlterPartitionReassignmentsNoOptionsRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.ADMIN_OWNERS;
    private static final String METHOD_NAME = "alterPartitionReassignments";
    private static final String OPTIONS_TYPE_TOKEN =
            "Lorg/apache/kafka/clients/admin/AlterPartitionReassignmentsOptions;";

    private final Severity severity;

    public AdminAlterPartitionReassignmentsNoOptionsRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_ALTER_PARTITION_REASSIGNMENTS_NO_OPTIONS;
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
                RuleId.ADMIN_ALTER_PARTITION_REASSIGNMENTS_NO_OPTIONS, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Admin.alterPartitionReassignments(Map<TopicPartition, "
                        + "Optional<NewPartitionReassignment>>) — a "
                        + "no-AlterPartitionReassignmentsOptions overload "
                        + "is reached here, either as a direct "
                        + "INVOKEINTERFACE on Admin / INVOKEVIRTUAL on "
                        + "AdminClient or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. "
                        + "`admin::alterPartitionReassignments` bound to "
                        + "Function<Map, AlterPartitionReassignmentsResult> "
                        + "or to a custom SAM whose erased implMethod "
                        + "descriptor matches the unsafe overload). "
                        + "alterPartitionReassignments is the KIP-455 "
                        + "write-side RPC — it submits a partition "
                        + "reassignment plan to the controller, which "
                        + "writes the /admin/reassignments znode (KRaft: "
                        + "the PartitionChangeRecord batch), triggers "
                        + "LeaderAndIsr requests to source/destination "
                        + "brokers, and begins migrating partition "
                        + "replicas. Once the controller has accepted "
                        + "the plan it is APPLIED — the only way to "
                        + "\"undo\" a reassignment is to submit a new "
                        + "reassignment that reverses it. There is no "
                        + "transactional rollback, no idempotency token; "
                        + "the operation modifies cluster topology in a "
                        + "way that is observable to every consumer and "
                        + "producer within seconds. The no-Options "
                        + "overload inherits the default "
                        + "request.timeout.ms (~30s) from the AdminClient "
                        + "configuration. The CONTROLLER is the entity "
                        + "handling the request, and the controller is "
                        + "exactly the entity that is busiest during "
                        + "high-traffic periods (ISR shrink/expand for "
                        + "lagging followers, broker session expiry) — "
                        + "those are the same windows during which an "
                        + "SRE most often initiates a reassignment (\"the "
                        + "cluster is unhealthy; rebalance it\"). When "
                        + "the future fails with TimeoutException the "
                        + "SRE tooling has NO way to distinguish \"the "
                        + "controller rejected the plan\" from \"the "
                        + "controller accepted the plan and is applying "
                        + "it but the response was lost\" from \"the "
                        + "request never reached the controller\". A "
                        + "naive retry-on-timeout implementation "
                        + "re-submits the plan — and the SECOND submit "
                        + "either no-ops (if the first was accepted) or "
                        + "COMPOUNDS the change (if the user has "
                        + "constructed the second plan from "
                        + "post-first-plan cluster state). Either way, "
                        + "the operator loses observability over the "
                        + "cluster's true topology state. Concrete "
                        + "failure modes: (1) duplicate-submission on "
                        + "naive retry — controller applies the plan "
                        + "twice, doubling the data movement; an SRE "
                        + "script wraps alterPartitionReassignments "
                        + "(plan) in a \"retry 3x on timeout\" "
                        + "decorator; the controller is busy; the first "
                        + "submit succeeds controller-side but the "
                        + "response is lost at the 30s boundary; the "
                        + "future completes exceptionally with "
                        + "TimeoutException; the retry decorator "
                        + "resubmits the same plan map; this time the "
                        + "call returns success; the user believes one "
                        + "reassignment was applied; in reality the "
                        + "controller saw the same plan twice; while "
                        + "the controller dedups identical plans for "
                        + "the same partition, it does NOT dedup "
                        + "overlapping plans — and if the SRE script "
                        + "regenerated the plan from live cluster state "
                        + "between attempts, the second plan references "
                        + "different replica sets and the controller "
                        + "applies BOTH, leading to two cascading "
                        + "rebalances and roughly 2x the inter-broker "
                        + "data movement; (2) atomicity loss — partial "
                        + "batch applied, caller sees failure; a "
                        + "reassignment plan covers 200 partitions "
                        + "across 5 topics; the controller begins "
                        + "writing the PartitionChangeRecord batch; "
                        + "partway through (partition 137) it processes "
                        + "a queued AlterIsr request and the write is "
                        + "delayed; the 30s timeout fires on the "
                        + "AdminClient side; the future completes "
                        + "exceptionally; the SRE script logs FAILED "
                        + "and aborts the deploy; meanwhile the "
                        + "controller finishes the batch — partitions "
                        + "0-199 have the new assignment (KRaft writes "
                        + "the whole batch atomically, but the SRE "
                        + "script doesn't know that); the script now "
                        + "believes the plan failed; the next deploy "
                        + "step depends on \"no reassignment is "
                        + "active\"; it polls "
                        + "listPartitionReassignments(), sees 200 "
                        + "active reassignments, and exits with "
                        + "unexpected cluster state, aborting; (3) "
                        + "reassignment-during-incident — caller is the "
                        + "healing-tool itself and timing out makes the "
                        + "incident worse; Cruise Control's "
                        + "anomaly-detector triggers a self-healing "
                        + "reassignment when a broker becomes "
                        + "unhealthy; the alterPartitionReassignments "
                        + "call inherits the default 30s timeout; the "
                        + "unhealthy broker is dragging down ISR "
                        + "membership, which is causing the controller "
                        + "to queue AlterIsr requests; the controller "
                        + "is contended; the self-healing submit times "
                        + "out; Cruise Control's anomaly-detector marks "
                        + "the healing action as failed and disables "
                        + "itself per the \"halt on repeated failure\" "
                        + "config — so the controller continues to "
                        + "shed brokers and the operator has just lost "
                        + "the automation that was supposed to mitigate "
                        + "the incident; (4) cancellation requires "
                        + "explicit empty-reassignment; a stuck call "
                        + "leaves the operator unable to reason about "
                        + "in-flight state — the intended way to "
                        + "cancel an in-flight reassignment is to call "
                        + "alterPartitionReassignments(Map.of(tp, "
                        + "Optional.empty())); if the cancellation "
                        + "call inherits the 30s timeout and the "
                        + "controller is busy processing the very "
                        + "reassignment the operator wants to cancel, "
                        + "the cancellation times out; the operator "
                        + "does not know whether the cancel was "
                        + "applied; they cannot safely submit a new "
                        + "plan because they do not know which "
                        + "partitions are still in motion; the cluster "
                        + "effectively stays in a frozen-rebalance "
                        + "state until human escalation re-establishes "
                        + "visibility; (5) INVOKEDYNAMIC method-"
                        + "reference captures bypass naive "
                        + "MethodInsnNode-only lint — Function<Map<"
                        + "TopicPartition, Optional<"
                        + "NewPartitionReassignment>>, "
                        + "AlterPartitionReassignmentsResult> submit = "
                        + "admin::alterPartitionReassignments captures "
                        + "an INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle on the 1-arg "
                        + "overload; the user-class bytecode contains "
                        + "zero direct INVOKEINTERFACE on the "
                        + "no-Options overload, only the indy site. "
                        + "Migration: pass an explicit "
                        + "AlterPartitionReassignmentsOptions pinning a "
                        + "per-call deadline that gives the controller "
                        + "enough time to durably accept the plan even "
                        + "under load — admin."
                        + "alterPartitionReassignments(plan, new "
                        + "AlterPartitionReassignmentsOptions()."
                        + "timeoutMs(120_000)). For an irreversible "
                        + "mutation the right move is to pick a "
                        + "timeout that is generous enough to almost "
                        + "always succeed controller-side, plus an "
                        + "idempotency strategy (submit-then-poll-"
                        + "listPartitionReassignments-to-confirm-"
                        + "acceptance rather than "
                        + "submit-then-retry-on-timeout).");
    }
}
