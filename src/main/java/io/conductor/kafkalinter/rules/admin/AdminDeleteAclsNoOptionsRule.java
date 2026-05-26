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
 * org.apache.kafka.clients.admin.Admin#deleteAcls
 * Admin.deleteAcls} overload that does NOT include a {@link
 * org.apache.kafka.clients.admin.DeleteAclsOptions
 * DeleteAclsOptions} argument. The single unsafe overload is
 * the 1-arg {@code deleteAcls(Collection<AclBindingFilter>)}.
 * The 2-arg Options-bearing overload is the safe form and is
 * intentionally not flagged.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} on {@link
 * org.apache.kafka.clients.admin.Admin Admin}, {@code
 * INVOKEVIRTUAL} on {@link
 * org.apache.kafka.clients.admin.AdminClient AdminClient}, and
 * {@code INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-Options {@code deleteAcls} is an IRREVERSIBLE
 * SECURITY production hazard</h2>
 *
 * <p>{@link
 * org.apache.kafka.clients.admin.Admin#deleteAcls
 * Admin.deleteAcls} is the canonical write-side RPC for
 * REMOVING ACL bindings from the authorizer. Unlike {@code
 * createAcls} where a missed binding just means "this caller
 * gets denied," {@code deleteAcls} accepts {@link
 * org.apache.kafka.common.acl.AclBindingFilter
 * AclBindingFilter} entries that can use {@code
 * PatternType.MATCH} and {@code AclOperation.ANY} and {@code
 * ResourceType.ANY} — a SINGLE filter like {@code
 * AclBindingFilter.ANY} matches and deletes EVERY ACL in the
 * cluster. The blast radius of one mis-scoped filter is the
 * whole authorizer.
 *
 * <p>The no-Options overload inherits the default {@code
 * request.timeout.ms} (~30 s) from the AdminClient
 * configuration. When the future fires {@code
 * TimeoutException} the caller cannot tell which subset of
 * matched bindings was deleted controller-side — and unlike
 * {@code createAcls} where the recovery is "re-create the
 * missing bindings," there is NO recovery for an unintended
 * delete: the original ACL grants are simply gone, and the
 * caller has no audit log of what was deleted unless they
 * captured {@code describeAcls()} BEFORE the delete and
 * persisted it.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Partial-batch deletion — controller deleted SOME
 *       bindings, AdminClient saw timeout, operator believes
 *       nothing happened.</b> An operator runs a one-time
 *       cleanup script that calls {@code deleteAcls(filters)}
 *       with 47 narrow filters targeting decommissioned
 *       service accounts. The authorizer is under load; the
 *       controller writes 23 of the 47 records before the
 *       30 s timeout fires AdminClient-side. The script logs
 *       FAILED and the operator believes no ACLs were
 *       removed. They re-run the script with the same 47
 *       filters; the controller deletes the remaining 24 +
 *       no-ops on the already-deleted 23. End state is
 *       correct — but for an hour between the two runs, 23
 *       service accounts had no ACLs and may have been
 *       authenticating-but-not-authorized on the cluster. If
 *       any of those service accounts were ACTIVE during
 *       that window, they failed with {@code
 *       TopicAuthorizationException} and the operator gets
 *       paged for an issue they thought they had safely
 *       deferred.</li>
 *   <li><b>Broad-filter delete catastrophe — single
 *       AclBindingFilter.ANY clears the whole authorizer
 *       before the timeout fires.</b> A bug in a Strimzi
 *       reconciliation loop submits {@code deleteAcls(List.
 *       of(AclBindingFilter.ANY))} — a single filter that
 *       matches every ACL. The controller dutifully begins
 *       deleting every binding cluster-wide; the
 *       AdminClient-side 30 s timeout fires partway through;
 *       the reconciliation loop catches the exception and
 *       logs WARN; the controller continues until it has
 *       deleted thousands of bindings; meanwhile every
 *       producer and consumer in the cluster starts failing
 *       with {@code TopicAuthorizationException}. The
 *       Strimzi resource never made it to {@code Failed}
 *       state because the WARN was treated as transient.
 *       SRE arrives to find a cluster-wide outage. Recovery
 *       requires restoring ACLs from the previous day's
 *       backup; bindings created since the backup are lost
 *       permanently.</li>
 *   <li><b>Cluster-wide retry is unsafe because deletes are
 *       not idempotent at the filter level.</b> A retry
 *       decorator sees {@code TimeoutException}, re-submits
 *       the same {@code filters} list. The first attempt
 *       deleted matching bindings; the second attempt
 *       matches the SAME filters but against the NEW
 *       (already-partially-deleted) cluster state — so it
 *       deletes the OTHER bindings that the filter matches
 *       now (possibly bindings that should not have been
 *       touched). This is a deletion that the operator did
 *       not request; it is silent because the second call
 *       returns success.</li>
 *   <li><b>describeAcls() pre-delete is the only audit trail
 *       and the no-Options timeout invalidates it.</b> A
 *       cautious operator runs {@code describeAcls(filter)}
 *       to capture a "before" snapshot, then {@code
 *       deleteAcls(filter)}, then {@code describeAcls(
 *       filter)} again to diff and log what was removed. If
 *       the delete times out mid-batch, the "after"
 *       describeAcls observes the partial state — and the
 *       diff log shows fewer deletions than actually
 *       happened controller-side (because the controller
 *       continued the batch after the AdminClient
 *       disconnected). The audit log lies about what the
 *       cluster actually does.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       cleanup helper built as {@code Function<Collection<
 *       AclBindingFilter>, DeleteAclsResult> delete = admin::
 *       deleteAcls} captures an {@code INVOKEDYNAMIC} whose
 *       bsm-args contain a {@code REF_invokeInterface}
 *       Handle on the 1-arg overload. The user-class
 *       bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Options overload, only
 *       the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.clients.admin.DeleteAclsOptions
 * DeleteAclsOptions} pinning a per-call deadline — e.g.
 * {@code admin.deleteAcls(filters, new DeleteAclsOptions().
 * timeoutMs(120_000))}. Additionally, NEVER use {@code
 * AclBindingFilter.ANY} or other broad filters — every
 * filter should pin a specific principal, resource pattern
 * type, and operation so that the blast radius of a
 * mis-scoped delete is bounded. And ALWAYS capture a
 * pre-delete {@code describeAcls(filter)} snapshot for the
 * audit trail before any production delete.
 */
public final class AdminDeleteAclsNoOptionsRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.ADMIN_OWNERS;
    private static final String METHOD_NAME = "deleteAcls";
    private static final String OPTIONS_TYPE_TOKEN =
            "Lorg/apache/kafka/clients/admin/DeleteAclsOptions;";

    private final Severity severity;

    public AdminDeleteAclsNoOptionsRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_DELETE_ACLS_NO_OPTIONS;
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
                RuleId.ADMIN_DELETE_ACLS_NO_OPTIONS, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Admin.deleteAcls(Collection<AclBindingFilter>) — a "
                        + "no-DeleteAclsOptions overload is reached here, "
                        + "either as a direct INVOKEINTERFACE on Admin / "
                        + "INVOKEVIRTUAL on AdminClient or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`admin::deleteAcls` bound to Function<"
                        + "Collection<AclBindingFilter>, DeleteAclsResult> "
                        + "or to a custom SAM whose erased implMethod "
                        + "descriptor matches the unsafe overload). "
                        + "deleteAcls is the canonical write-side RPC for "
                        + "REMOVING ACL bindings from the authorizer. "
                        + "Unlike createAcls where a missed binding just "
                        + "means \"this caller gets denied,\" deleteAcls "
                        + "accepts AclBindingFilter entries that can use "
                        + "PatternType.MATCH and AclOperation.ANY and "
                        + "ResourceType.ANY — a SINGLE filter like "
                        + "AclBindingFilter.ANY matches and deletes EVERY "
                        + "ACL in the cluster. The blast radius of one "
                        + "mis-scoped filter is the whole authorizer. The "
                        + "no-Options overload inherits the default "
                        + "request.timeout.ms (~30s) from the AdminClient "
                        + "configuration. When the future fires "
                        + "TimeoutException the caller cannot tell which "
                        + "subset of matched bindings was deleted "
                        + "controller-side — and unlike createAcls where "
                        + "the recovery is \"re-create the missing "
                        + "bindings,\" there is NO recovery for an "
                        + "unintended delete: the original ACL grants are "
                        + "simply gone, and the caller has no audit log "
                        + "of what was deleted unless they captured "
                        + "describeAcls() BEFORE the delete and persisted "
                        + "it. Concrete failure modes: (1) partial-batch "
                        + "deletion — controller deleted SOME bindings, "
                        + "AdminClient saw timeout, operator believes "
                        + "nothing happened; an operator runs a one-time "
                        + "cleanup script that calls deleteAcls(filters) "
                        + "with 47 narrow filters targeting "
                        + "decommissioned service accounts; the "
                        + "authorizer is under load; the controller "
                        + "writes 23 of the 47 records before the 30s "
                        + "timeout fires AdminClient-side; the script "
                        + "logs FAILED and the operator believes no ACLs "
                        + "were removed; they re-run the script with the "
                        + "same 47 filters; the controller deletes the "
                        + "remaining 24 + no-ops on the already-deleted "
                        + "23; end state is correct — but for an hour "
                        + "between the two runs, 23 service accounts had "
                        + "no ACLs and may have been "
                        + "authenticating-but-not-authorized on the "
                        + "cluster; if any of those service accounts "
                        + "were ACTIVE during that window, they failed "
                        + "with TopicAuthorizationException and the "
                        + "operator gets paged for an issue they thought "
                        + "they had safely deferred; (2) broad-filter "
                        + "delete catastrophe — single "
                        + "AclBindingFilter.ANY clears the whole "
                        + "authorizer before the timeout fires; a bug in "
                        + "a Strimzi reconciliation loop submits "
                        + "deleteAcls(List.of(AclBindingFilter.ANY)) — a "
                        + "single filter that matches every ACL; the "
                        + "controller dutifully begins deleting every "
                        + "binding cluster-wide; the AdminClient-side "
                        + "30s timeout fires partway through; the "
                        + "reconciliation loop catches the exception and "
                        + "logs WARN; the controller continues until it "
                        + "has deleted thousands of bindings; meanwhile "
                        + "every producer and consumer in the cluster "
                        + "starts failing with "
                        + "TopicAuthorizationException; the Strimzi "
                        + "resource never made it to Failed state "
                        + "because the WARN was treated as transient; "
                        + "SRE arrives to find a cluster-wide outage; "
                        + "recovery requires restoring ACLs from the "
                        + "previous day's backup; bindings created since "
                        + "the backup are lost permanently; (3) "
                        + "cluster-wide retry is unsafe because deletes "
                        + "are not idempotent at the filter level — a "
                        + "retry decorator sees TimeoutException, "
                        + "re-submits the same filters list; the first "
                        + "attempt deleted matching bindings; the second "
                        + "attempt matches the SAME filters but against "
                        + "the NEW (already-partially-deleted) cluster "
                        + "state — so it deletes the OTHER bindings "
                        + "that the filter matches now (possibly "
                        + "bindings that should not have been touched); "
                        + "this is a deletion that the operator did not "
                        + "request; it is silent because the second "
                        + "call returns success; (4) describeAcls() "
                        + "pre-delete is the only audit trail and the "
                        + "no-Options timeout invalidates it — a "
                        + "cautious operator runs describeAcls(filter) "
                        + "to capture a \"before\" snapshot, then "
                        + "deleteAcls(filter), then describeAcls(filter) "
                        + "again to diff and log what was removed; if "
                        + "the delete times out mid-batch, the \"after\" "
                        + "describeAcls observes the partial state — "
                        + "and the diff log shows fewer deletions than "
                        + "actually happened controller-side (because "
                        + "the controller continued the batch after the "
                        + "AdminClient disconnected); the audit log "
                        + "lies about what the cluster actually does; "
                        + "(5) INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "Function<Collection<AclBindingFilter>, "
                        + "DeleteAclsResult> delete = admin::deleteAcls "
                        + "captures an INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle on the "
                        + "1-arg overload; the user-class bytecode "
                        + "contains zero direct INVOKEINTERFACE on the "
                        + "no-Options overload, only the indy site. "
                        + "Migration: pass an explicit DeleteAclsOptions "
                        + "pinning a per-call deadline — admin."
                        + "deleteAcls(filters, new DeleteAclsOptions()."
                        + "timeoutMs(120_000)). Additionally, NEVER use "
                        + "AclBindingFilter.ANY or other broad filters "
                        + "— every filter should pin a specific "
                        + "principal, resource pattern type, and "
                        + "operation so that the blast radius of a "
                        + "mis-scoped delete is bounded. And ALWAYS "
                        + "capture a pre-delete describeAcls(filter) "
                        + "snapshot for the audit trail before any "
                        + "production delete.");
    }
}
