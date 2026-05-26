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
 * org.apache.kafka.clients.admin.Admin#listPartitionReassignments()
 * Admin.listPartitionReassignments} overload that does NOT
 * include a {@link
 * org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions
 * ListPartitionReassignmentsOptions} argument. The two unsafe
 * overloads are the 0-arg {@code listPartitionReassignments()}
 * and the 1-arg {@code listPartitionReassignments(Set<
 * TopicPartition>)}. The Options-bearing overloads are the
 * safe form and are intentionally not flagged.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} on {@link
 * org.apache.kafka.clients.admin.Admin Admin}, {@code
 * INVOKEVIRTUAL} on {@link
 * org.apache.kafka.clients.admin.AdminClient AdminClient}, and
 * {@code INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-Options {@code listPartitionReassignments()} is a
 * production-diagnostics hazard during exactly the events the
 * call was meant to observe</h2>
 *
 * <p>{@link
 * org.apache.kafka.clients.admin.Admin#listPartitionReassignments()
 * Admin.listPartitionReassignments()} is the KIP-455
 * progress-diagnostic — the canonical way to ask the
 * controller "which partitions are currently being reassigned
 * and how far along are they?" Cruise Control, Kafka
 * Rebalance, Strimzi's UserOperator, and most internal
 * SRE-built rebalance dashboards all poll this method
 * repeatedly during long-running reassignments (which routinely
 * take HOURS for multi-TB topics) to render progress bars and
 * to gate downstream operations on completion.
 *
 * <p>The catch is that the no-Options overloads inherit the
 * default {@code request.timeout.ms} (~30 s) from the
 * AdminClient configuration. The CONTROLLER is the entity
 * responding to the {@code ListPartitionReassignments} request
 * — and the controller is exactly the entity that is BUSY
 * during a rebalance: it is shuttling partition metadata,
 * sending {@code LeaderAndIsr} requests to brokers, processing
 * {@code AlterIsr} requests from brokers, etc. The contended
 * controller is precisely the state in which the polling
 * caller wants visibility — and precisely the state in which
 * the 30 s default timeout fires.
 *
 * <p>When the future fails with {@code TimeoutException}, the
 * caller has NO way to distinguish "controller is unreachable"
 * from "controller is busy applying reassignments, request
 * queued behind LeaderAndIsr writes" from "request never
 * dispatched." The dashboard's progress bar goes red; the
 * operator escalates; L2 arrives to find the rebalance was
 * progressing fine all along and the only problem was the
 * caller's timeout.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Cruise Control / Kafka Rebalance polling stalls
 *       during exactly the rebalance it is supposed to
 *       observe.</b> A rebalance plan moves 8 TB of partition
 *       data across 12 brokers; expected completion is 6
 *       hours. The Rebalance operator polls {@code
 *       listPartitionReassignments()} every 30 s to render
 *       "X% complete" on its dashboard. At hour 3 the
 *       controller is processing a burst of {@code AlterIsr}
 *       requests from brokers committing finished moves; the
 *       next {@code listPartitionReassignments} request waits
 *       behind those writes; the 30 s timeout fires; the
 *       Rebalance operator marks the rebalance "stalled" and
 *       fires a P2 alert. SRE picks up the page, looks at the
 *       cluster, finds the rebalance is progressing normally,
 *       and resolves the alert as a false positive. Future
 *       alerts are taken less seriously.</li>
 *   <li><b>0-arg overload returns EVERY active reassignment
 *       cluster-wide, inflating response size.</b> {@code
 *       listPartitionReassignments()} (no arguments)
 *       enumerates every in-flight reassignment across the
 *       cluster. On a large cluster with many independent
 *       reassignments active simultaneously (e.g. multiple
 *       teams' Strimzi-managed clusters all rebalancing at
 *       once) the response payload can be hundreds of KB.
 *       The default 30 s timeout becomes 30 s waiting for the
 *       controller to serialize and ship that payload. The
 *       {@code listPartitionReassignments(Set<TopicPartition>,
 *       Options)} overload lets the caller scope the query
 *       to their own partitions, dropping the payload by 100×
 *       and making the call cheap.</li>
 *   <li><b>Strimzi's KafkaRebalance controller marks
 *       NotReady on transient timeouts, blocking dependent
 *       resources.</b> The Strimzi {@code KafkaRebalance}
 *       custom resource controller wraps {@code
 *       listPartitionReassignments()} for status reporting.
 *       When the call times out the Kubernetes resource
 *       transitions to {@code NotReady}; any {@code Deployment}
 *       that has a {@code dependsOn} on this resource (via
 *       initContainer waiting for the resource) is blocked.
 *       A 30 s controller-side blip becomes a multi-minute
 *       deployment-blocking incident.</li>
 *   <li><b>Polling loops without explicit timeout become
 *       unbounded-retry loops on broker outage.</b> A simple
 *       {@code while (true) admin.listPartitionReassignments().
 *       all().get(); Thread.sleep(30_000);} loop is intended
 *       to poll forever. When the controller is unreachable,
 *       each call hangs for the default 30 s timeout then
 *       throws — the loop catches, sleeps 30 s, retries.
 *       That's a 60 s effective cycle while the cluster is
 *       broken, doubling the time-to-detect. With an explicit
 *       2-minute timeout the loop knows to fall back to a
 *       slower probe cadence (e.g. 5-minute polls during
 *       broker-unreachable state) and surface the issue
 *       to the dashboard.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       dashboard poller built as {@code Supplier<
 *       ListPartitionReassignmentsResult> probe = admin::
 *       listPartitionReassignments} captures an {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle on {@code
 *       Admin.listPartitionReassignments()
 *       ListPartitionReassignmentsResult}. The user-class
 *       bytecode contains zero direct {@code INVOKEINTERFACE}
 *       on the no-Options overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions
 * ListPartitionReassignmentsOptions} pinning a per-call
 * deadline appropriate to the polling cadence — e.g.
 * {@code admin.listPartitionReassignments(myPartitions, new
 * ListPartitionReassignmentsOptions().timeoutMs(120_000))} for
 * a 30 s polling loop on a busy controller. Prefer the
 * {@code (Set<TopicPartition>, Options)} overload when the
 * caller only cares about its own partitions — both for
 * latency (smaller response) and for cross-tenant safety
 * (does not leak other teams' reassignments to this caller).
 */
public final class AdminListPartitionReassignmentsNoOptionsRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.ADMIN_OWNERS;
    private static final String METHOD_NAME = "listPartitionReassignments";
    private static final String OPTIONS_TYPE_TOKEN =
            "Lorg/apache/kafka/clients/admin/ListPartitionReassignmentsOptions;";

    private final Severity severity;

    public AdminListPartitionReassignmentsNoOptionsRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_LIST_PARTITION_REASSIGNMENTS_NO_OPTIONS;
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
                RuleId.ADMIN_LIST_PARTITION_REASSIGNMENTS_NO_OPTIONS, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Admin.listPartitionReassignments() or "
                        + "listPartitionReassignments(Set<TopicPartition>) "
                        + "— a no-ListPartitionReassignmentsOptions "
                        + "overload is reached here, either as a direct "
                        + "INVOKEINTERFACE on Admin / INVOKEVIRTUAL on "
                        + "AdminClient or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. "
                        + "`admin::listPartitionReassignments` bound to "
                        + "Supplier<ListPartitionReassignmentsResult> or "
                        + "to a custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). "
                        + "listPartitionReassignments is the KIP-455 "
                        + "progress-diagnostic — the canonical way to "
                        + "ask the controller \"which partitions are "
                        + "currently being reassigned and how far along "
                        + "are they?\". Cruise Control, Kafka Rebalance, "
                        + "Strimzi's UserOperator, and most internal "
                        + "SRE-built rebalance dashboards all poll this "
                        + "method repeatedly during long-running "
                        + "reassignments (routinely HOURS for multi-TB "
                        + "topics) to render progress bars and to gate "
                        + "downstream operations on completion. The "
                        + "catch is that the no-Options overloads "
                        + "inherit the default request.timeout.ms (~30s) "
                        + "from the AdminClient configuration. The "
                        + "CONTROLLER is the entity responding to the "
                        + "ListPartitionReassignments request — and the "
                        + "controller is exactly the entity that is BUSY "
                        + "during a rebalance (shuttling partition "
                        + "metadata, sending LeaderAndIsr requests to "
                        + "brokers, processing AlterIsr requests from "
                        + "brokers). The contended controller is "
                        + "precisely the state in which the polling "
                        + "caller wants visibility — and precisely the "
                        + "state in which the 30s default timeout "
                        + "fires. When the future fails with "
                        + "TimeoutException, the caller has NO way to "
                        + "distinguish \"controller is unreachable\" "
                        + "from \"controller is busy applying "
                        + "reassignments, request queued behind "
                        + "LeaderAndIsr writes\" from \"request never "
                        + "dispatched\". The dashboard's progress bar "
                        + "goes red, the operator escalates, L2 arrives "
                        + "to find the rebalance was progressing fine "
                        + "all along and the only problem was the "
                        + "caller's timeout. Concrete failure modes: "
                        + "(1) Cruise Control / Kafka Rebalance polling "
                        + "stalls during exactly the rebalance it is "
                        + "supposed to observe — a rebalance plan moves "
                        + "8 TB of partition data across 12 brokers, "
                        + "expected completion 6 hours; the Rebalance "
                        + "operator polls listPartitionReassignments() "
                        + "every 30s to render % complete on its "
                        + "dashboard; at hour 3 the controller is "
                        + "processing a burst of AlterIsr requests from "
                        + "brokers committing finished moves; the next "
                        + "listPartitionReassignments request waits "
                        + "behind those writes; the 30s timeout fires; "
                        + "the Rebalance operator marks the rebalance "
                        + "stalled and fires a P2 alert; SRE picks up "
                        + "the page, looks at the cluster, finds the "
                        + "rebalance is progressing normally and "
                        + "resolves the alert as a false positive; "
                        + "future alerts are taken less seriously; (2) "
                        + "0-arg overload returns EVERY active "
                        + "reassignment cluster-wide inflating response "
                        + "size — listPartitionReassignments() (no "
                        + "arguments) enumerates every in-flight "
                        + "reassignment across the cluster; on a large "
                        + "cluster with many independent reassignments "
                        + "active simultaneously (multiple teams' "
                        + "Strimzi-managed clusters all rebalancing at "
                        + "once) the response payload can be hundreds "
                        + "of KB; the default 30s timeout becomes 30s "
                        + "waiting for the controller to serialize and "
                        + "ship that payload; the (Set<TopicPartition>, "
                        + "Options) overload lets the caller scope the "
                        + "query to their own partitions, dropping the "
                        + "payload by 100x and making the call cheap; "
                        + "(3) Strimzi's KafkaRebalance controller "
                        + "marks NotReady on transient timeouts, "
                        + "blocking dependent resources — the Strimzi "
                        + "KafkaRebalance custom resource controller "
                        + "wraps listPartitionReassignments() for "
                        + "status reporting; when the call times out "
                        + "the Kubernetes resource transitions to "
                        + "NotReady; any Deployment with a dependsOn on "
                        + "this resource (via initContainer waiting "
                        + "for the resource) is blocked; a 30s "
                        + "controller-side blip becomes a multi-minute "
                        + "deployment-blocking incident; (4) polling "
                        + "loops without explicit timeout become "
                        + "unbounded-retry loops on broker outage — a "
                        + "simple while (true) admin."
                        + "listPartitionReassignments().all().get(); "
                        + "Thread.sleep(30_000); loop is intended to "
                        + "poll forever; when the controller is "
                        + "unreachable each call hangs for the default "
                        + "30s timeout then throws, the loop catches, "
                        + "sleeps 30s, retries — a 60s effective cycle "
                        + "while the cluster is broken, doubling the "
                        + "time-to-detect; with an explicit 2-minute "
                        + "timeout the loop knows to fall back to a "
                        + "slower probe cadence (e.g. 5-minute polls "
                        + "during broker-unreachable state) and "
                        + "surface the issue to the dashboard; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "Supplier<ListPartitionReassignmentsResult> "
                        + "probe = admin::listPartitionReassignments "
                        + "captures an INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle on "
                        + "Admin.listPartitionReassignments()"
                        + "ListPartitionReassignmentsResult; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Options overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit ListPartitionReassignmentsOptions "
                        + "pinning a per-call deadline appropriate to "
                        + "the polling cadence — admin."
                        + "listPartitionReassignments(myPartitions, "
                        + "new ListPartitionReassignmentsOptions()."
                        + "timeoutMs(120_000)) for a 30s polling loop "
                        + "on a busy controller. Prefer the (Set<"
                        + "TopicPartition>, Options) overload when the "
                        + "caller only cares about its own partitions "
                        + "— both for latency (smaller response) and "
                        + "for cross-tenant safety (does not leak "
                        + "other teams' reassignments to this caller).");
    }
}
