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
 * org.apache.kafka.clients.admin.Admin#createAcls
 * Admin.createAcls} overload that does NOT include a {@link
 * org.apache.kafka.clients.admin.CreateAclsOptions
 * CreateAclsOptions} argument. The single unsafe overload is
 * the 1-arg {@code createAcls(Collection<AclBinding>)}. The
 * 2-arg Options-bearing overload is the safe form and is
 * intentionally not flagged.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} on {@link
 * org.apache.kafka.clients.admin.Admin Admin}, {@code
 * INVOKEVIRTUAL} on {@link
 * org.apache.kafka.clients.admin.AdminClient AdminClient}, and
 * {@code INVOKEDYNAMIC} method-reference captures.
 *
 * <h2>Why no-Options {@code createAcls} is a SECURITY
 * production hazard</h2>
 *
 * <p>{@link
 * org.apache.kafka.clients.admin.Admin#createAcls
 * Admin.createAcls} is the canonical write-side RPC for
 * inserting ACL bindings into the authorizer (KRaft: a
 * {@code AccessControlEntryRecord} batch; ZK: znode writes
 * under {@code /kafka-acl}). ACLs are the gate that controls
 * who can produce to, consume from, or describe a topic; a
 * provisioning step that "succeeded" in tooling but did NOT
 * actually persist the binding leaves callers without
 * authorization — and the next time the application tries to
 * use the cluster, it fails with {@code
 * TopicAuthorizationException}.
 *
 * <p>The no-Options overload inherits the default {@code
 * request.timeout.ms} (~30 s) from the AdminClient
 * configuration. Critically, {@code createAcls} accepts a
 * BATCH of bindings — and the per-binding futures inside
 * {@link
 * org.apache.kafka.clients.admin.CreateAclsResult
 * CreateAclsResult} resolve INDEPENDENTLY. When the wrapping
 * future returned by {@code all().get()} fires {@code
 * TimeoutException} the caller cannot tell which subset of
 * bindings was persisted controller-side. {@code values()}
 * exposes the per-binding map but most production code
 * incorrectly calls {@code all()} for a single success/fail
 * signal — and a timeout on {@code all()} discards the
 * per-binding state entirely. The cluster ends in a
 * partial-ACL state: SOME of the requested bindings are
 * live, the rest are not, the caller has zero observability
 * over which is which.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Partial-batch persistence — application starts up
 *       and CAN write to half its topics but not the
 *       other.</b> A deployment provisions ACLs for a new
 *       microservice across 12 topics in one {@code
 *       createAcls(bindings)} call. The authorizer is under
 *       load (large fleet, many ACL changes from other
 *       teams); the controller writes 7 of the 12 records
 *       before the 30 s timeout fires AdminClient-side. The
 *       deploy script catches the {@code TimeoutException},
 *       logs FAILED, and retries the WHOLE batch. The second
 *       attempt re-creates the 7 already-applied bindings
 *       (no-op) AND attempts the 5 remaining; this time it
 *       succeeds. But if the operator did NOT retry and
 *       instead marked the deploy as failed, the microservice
 *       starts up in production and gets {@code
 *       TopicAuthorizationException} on the 5 topics that
 *       never got their bindings — and the SRE has to debug
 *       why "half" the topics are authorized.</li>
 *   <li><b>{@code all()} masks per-binding failures — caller
 *       cannot distinguish timeout from validation error.</b>
 *       Production code routinely calls {@code admin.
 *       createAcls(bindings).all().get()} to get a single
 *       success/fail signal. The {@code all()} future fires
 *       exceptionally on the FIRST failure — if binding 3 of
 *       12 is invalid (e.g. unknown principal type) {@code
 *       all()} throws {@code InvalidRequestException} and
 *       the caller never sees that bindings 1, 2, 4–12 may
 *       have been persisted. With a tight timeout the caller
 *       cannot distinguish "binding 3 was invalid" from
 *       "controller timed out partway through batch" — and
 *       both leave the cluster in a partial-ACL state that
 *       the caller has no map for.</li>
 *   <li><b>Provisioning idempotency assumption breaks when
 *       the controller persists but the AdminClient times
 *       out.</b> A GitOps reconciliation loop ({@code
 *       Strimzi KafkaUser}, Confluent Operator) treats {@code
 *       createAcls} as idempotent — "if it's there, no-op;
 *       if it's not, create it." The loop calls {@code
 *       createAcls(allBindings)}, gets a 30 s timeout, marks
 *       the resource {@code NotReady}, and re-enters the
 *       reconcile loop. On the next pass it queries {@code
 *       describeAcls()}, sees that all the bindings DID get
 *       created controller-side, and now believes everything
 *       is fine — except the controller actually got the
 *       create twice and silently dedup'd the second; meaning
 *       any drift between the two calls (e.g. someone removed
 *       a binding between attempts) is silently lost.</li>
 *   <li><b>Compliance audit fails after a "successful"
 *       provisioning run because half the ACLs are missing.</b>
 *       A bank's quarterly SOC2 audit checks "every service
 *       account has the LEAST PRIVILEGE binding for its
 *       topics — neither broader nor narrower than the
 *       config-as-code says." The provisioning system ran 6
 *       weeks ago, hit a timeout midway through a 200-binding
 *       batch, logged success-with-warnings, and continued.
 *       The audit finds 47 missing bindings; the engineering
 *       team cannot explain why "we ran the script" did not
 *       persist them all; the audit is escalated to "material
 *       finding."</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       provisioning helper built as {@code Function<
 *       Collection<AclBinding>, CreateAclsResult> create =
 *       admin::createAcls} captures an {@code INVOKEDYNAMIC}
 *       whose bsm-args contain a {@code REF_invokeInterface}
 *       Handle on the 1-arg overload. The user-class
 *       bytecode contains zero direct {@code
 *       INVOKEINTERFACE} on the no-Options overload, only
 *       the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@link
 * org.apache.kafka.clients.admin.CreateAclsOptions
 * CreateAclsOptions} pinning a per-call deadline — e.g.
 * {@code admin.createAcls(bindings, new CreateAclsOptions().
 * timeoutMs(120_000))}. Additionally, ALWAYS iterate {@code
 * result.values().entrySet()} (per-binding futures) rather
 * than relying on {@code result.all()} — so that a partial
 * timeout still yields a per-binding success/fail map and
 * the caller can re-attempt only the missed bindings.
 */
public final class AdminCreateAclsNoOptionsRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.ADMIN_OWNERS;
    private static final String METHOD_NAME = "createAcls";
    private static final String OPTIONS_TYPE_TOKEN =
            "Lorg/apache/kafka/clients/admin/CreateAclsOptions;";

    private final Severity severity;

    public AdminCreateAclsNoOptionsRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_CREATE_ACLS_NO_OPTIONS;
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
                RuleId.ADMIN_CREATE_ACLS_NO_OPTIONS, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Admin.createAcls(Collection<AclBinding>) — a "
                        + "no-CreateAclsOptions overload is reached here, "
                        + "either as a direct INVOKEINTERFACE on Admin / "
                        + "INVOKEVIRTUAL on AdminClient or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`admin::createAcls` bound to Function<"
                        + "Collection<AclBinding>, CreateAclsResult> or to "
                        + "a custom SAM whose erased implMethod descriptor "
                        + "matches the unsafe overload). createAcls is the "
                        + "canonical write-side RPC for inserting ACL "
                        + "bindings into the authorizer (KRaft: an "
                        + "AccessControlEntryRecord batch; ZK: znode "
                        + "writes under /kafka-acl). ACLs are the gate "
                        + "that controls who can produce to, consume from, "
                        + "or describe a topic; a provisioning step that "
                        + "\"succeeded\" in tooling but did NOT actually "
                        + "persist the binding leaves callers without "
                        + "authorization — and the next time the "
                        + "application tries to use the cluster, it fails "
                        + "with TopicAuthorizationException. The "
                        + "no-Options overload inherits the default "
                        + "request.timeout.ms (~30s) from the AdminClient "
                        + "configuration. Critically, createAcls accepts "
                        + "a BATCH of bindings — and the per-binding "
                        + "futures inside CreateAclsResult resolve "
                        + "INDEPENDENTLY. When the wrapping future "
                        + "returned by all().get() fires TimeoutException "
                        + "the caller cannot tell which subset of bindings "
                        + "was persisted controller-side. values() "
                        + "exposes the per-binding map but most production "
                        + "code incorrectly calls all() for a single "
                        + "success/fail signal — and a timeout on all() "
                        + "discards the per-binding state entirely. The "
                        + "cluster ends in a partial-ACL state: SOME of "
                        + "the requested bindings are live, the rest are "
                        + "not, the caller has zero observability over "
                        + "which is which. Concrete failure modes: (1) "
                        + "partial-batch persistence — application starts "
                        + "up and CAN write to half its topics but not "
                        + "the other; a deployment provisions ACLs for a "
                        + "new microservice across 12 topics in one "
                        + "createAcls(bindings) call; the authorizer is "
                        + "under load (large fleet, many ACL changes from "
                        + "other teams); the controller writes 7 of the "
                        + "12 records before the 30s timeout fires "
                        + "AdminClient-side; the deploy script catches "
                        + "the TimeoutException, logs FAILED, and retries "
                        + "the WHOLE batch; the second attempt re-creates "
                        + "the 7 already-applied bindings (no-op) AND "
                        + "attempts the 5 remaining; this time it "
                        + "succeeds; but if the operator did NOT retry "
                        + "and instead marked the deploy as failed, the "
                        + "microservice starts up in production and gets "
                        + "TopicAuthorizationException on the 5 topics "
                        + "that never got their bindings — and the SRE "
                        + "has to debug why half the topics are "
                        + "authorized; (2) all() masks per-binding "
                        + "failures — caller cannot distinguish timeout "
                        + "from validation error; production code "
                        + "routinely calls admin.createAcls(bindings)."
                        + "all().get() to get a single success/fail "
                        + "signal; the all() future fires exceptionally "
                        + "on the FIRST failure — if binding 3 of 12 is "
                        + "invalid (unknown principal type) all() throws "
                        + "InvalidRequestException and the caller never "
                        + "sees that bindings 1, 2, 4-12 may have been "
                        + "persisted; with a tight timeout the caller "
                        + "cannot distinguish binding 3 was invalid from "
                        + "controller timed out partway through batch — "
                        + "and both leave the cluster in a partial-ACL "
                        + "state the caller has no map for; (3) "
                        + "provisioning idempotency assumption breaks "
                        + "when the controller persists but the "
                        + "AdminClient times out; a GitOps reconciliation "
                        + "loop (Strimzi KafkaUser, Confluent Operator) "
                        + "treats createAcls as idempotent (if it's "
                        + "there, no-op; if it's not, create it); the "
                        + "loop calls createAcls(allBindings), gets a 30s "
                        + "timeout, marks the resource NotReady, and "
                        + "re-enters the reconcile loop; on the next pass "
                        + "it queries describeAcls(), sees that all the "
                        + "bindings DID get created controller-side, and "
                        + "now believes everything is fine — except the "
                        + "controller actually got the create twice and "
                        + "silently dedup'd the second; meaning any "
                        + "drift between the two calls (someone removed "
                        + "a binding between attempts) is silently lost; "
                        + "(4) compliance audit fails after a "
                        + "successful provisioning run because half the "
                        + "ACLs are missing; a bank's quarterly SOC2 "
                        + "audit checks every service account has the "
                        + "LEAST PRIVILEGE binding for its topics; the "
                        + "provisioning system ran 6 weeks ago, hit a "
                        + "timeout midway through a 200-binding batch, "
                        + "logged success-with-warnings, and continued; "
                        + "the audit finds 47 missing bindings; the "
                        + "engineering team cannot explain why we ran "
                        + "the script did not persist them all; the "
                        + "audit is escalated to material finding; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "Function<Collection<AclBinding>, "
                        + "CreateAclsResult> create = admin::createAcls "
                        + "captures an INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle on the "
                        + "1-arg overload; the user-class bytecode "
                        + "contains zero direct INVOKEINTERFACE on the "
                        + "no-Options overload, only the indy site. "
                        + "Migration: pass an explicit CreateAclsOptions "
                        + "pinning a per-call deadline — admin."
                        + "createAcls(bindings, new CreateAclsOptions()."
                        + "timeoutMs(120_000)). Additionally, ALWAYS "
                        + "iterate result.values().entrySet() "
                        + "(per-binding futures) rather than relying on "
                        + "result.all() — so that a partial timeout "
                        + "still yields a per-binding success/fail map "
                        + "and the caller can re-attempt only the "
                        + "missed bindings.");
    }
}
