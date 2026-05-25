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
 * Fires for every reach of an
 * {@link org.apache.kafka.clients.admin.Admin#createTopics(java.util.Collection)
 * Admin.createTopics(Collection)} overload that does NOT include a
 * {@link org.apache.kafka.clients.admin.CreateTopicsOptions
 * CreateTopicsOptions} argument and therefore falls back to the
 * default {@code request.timeout.ms} (~30 s) with no caller-
 * visible bound on the operation.
 *
 * <p>Predicate is structural — any descriptor for {@code
 * createTopics} on {@code Admin} or {@code AdminClient} whose
 * argument list contains
 * {@code org/apache/kafka/clients/admin/CreateTopicsOptions} is
 * safe; any descriptor that does not is unsafe.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls on {@link
 * org.apache.kafka.clients.admin.Admin Admin} (the interface),
 * {@code INVOKEVIRTUAL} calls on {@link
 * org.apache.kafka.clients.admin.AdminClient AdminClient} (the
 * abstract class), and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code admin::createTopics} bound to {@link
 * java.util.function.Function
 * Function&lt;Collection, CreateTopicsResult&gt;} or to a custom
 * SAM whose erased implMethod descriptor matches an unsafe
 * overload).
 *
 * <h2>Why no-Options createTopics is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.admin.Admin#createTopics(
 * java.util.Collection) Admin.createTopics} is asynchronous: it
 * issues a CreateTopics request to the controller and returns a
 * future. Without an explicit {@link
 * org.apache.kafka.clients.admin.CreateTopicsOptions}, the call
 * uses {@code request.timeout.ms} (default ~30 s) as the
 * deadline for the controller to acknowledge. When the future
 * times out the caller sees a {@code TimeoutException} — but
 * that exception does NOT mean the topic was not created. The
 * controller may still be processing the request; the topic may
 * appear on the cluster seconds after the timeout fires. The
 * caller has no way to distinguish "request never reached the
 * controller" from "request reached the controller, controller
 * is still working on it" from "topic was created seconds
 * ago".
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Cluster bring-up scripts double-create on retry.</b>
 *       A bootstrap script calls {@code admin.createTopics(
 *       List.of(new NewTopic("orders", 12, (short) 3)))} as
 *       part of provisioning. The controller is under load
 *       (large rebalance in flight); the call times out at
 *       30 s; the script catches {@code TimeoutException} and
 *       retries with the same topic spec. The second call
 *       observes {@code TopicExistsException} because the first
 *       request actually succeeded after the timeout. The
 *       script's retry logic was not written to expect that
 *       race (it expected idempotent retries on transient
 *       failures), so the bootstrap fails. SRE rolls back the
 *       deploy and the cluster is left in a half-provisioned
 *       state with one topic created and zero ACLs applied.</li>
 *   <li><b>Mid-incident topic-creation deadlines exceed oncall
 *       runbook time-boxes.</b> During a production incident an
 *       SRE creates a quarantine topic to redirect a poison-
 *       pill stream. The runbook says "create the quarantine
 *       topic and confirm within 90 seconds, otherwise escalate
 *       to L2". The {@code createTopics()} call times out at 30
 *       s with no way for the SRE to extend the bound from the
 *       call site. They can't tell whether the topic was
 *       created or not (the future is failed; checking with
 *       describeTopics adds another 30 s); they escalate; L2
 *       arrives to find the topic was created 15 s before they
 *       picked up the page.</li>
 *   <li><b>CI/CD topic-provisioning jobs flake under controller
 *       load.</b> CI provisions a per-PR namespace of topics
 *       via a single createTopics call. As the test cluster
 *       scales (more PRs in flight = more controller
 *       contention), the default 30 s timeout starts catching
 *       the tail of legitimate operations; the CI step fails
 *       intermittently with {@code TimeoutException}; engineers
 *       click "retry" until it passes; the apparent flake
 *       distribution hides a real cluster-load problem until
 *       the controller falls over.</li>
 *   <li><b>Operator can't bound the operation from outside.</b>
 *       Without {@code CreateTopicsOptions.timeoutMs(long)} on
 *       the call, the deadline is determined by whatever
 *       {@code request.timeout.ms} the AdminClient was
 *       configured with at construction time — typically
 *       global config baked into a shared library. Tuning
 *       per-call deadlines (e.g. "this provisioning step has a
 *       2-minute budget; the next probe step has a 5-second
 *       budget") requires editing the AdminClient construction
 *       (and any caching it sits behind) rather than the call
 *       site. The caller has no local control over the
 *       operation deadline.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> An
 *       admin facade built as {@code Function&lt;Collection
 *       &lt;NewTopic&gt;, CreateTopicsResult&gt; provisioner =
 *       admin::createTopics} compiles to {@code INVOKEDYNAMIC}
 *       whose bsm-args contain a {@code REF_invokeInterface}
 *       Handle pointing at {@code Admin.createTopics(Collection)
 *       CreateTopicsResult}. The user-class bytecode contains
 *       zero direct {@code INVOKEINTERFACE} on the no-Options
 *       overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@code CreateTopicsOptions}
 * pinning a per-call deadline — e.g. {@code admin.createTopics(
 * List.of(new NewTopic("orders", 12, (short) 3)), new
 * CreateTopicsOptions().timeoutMs(60_000))}. The bound is
 * local to the call site and explicitly chosen, so retry logic
 * upstream can distinguish "in flight" from "failed" by
 * comparing against the chosen deadline.
 */
public final class AdminCreateTopicsNoOptionsRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.ADMIN_OWNERS;
    private static final String METHOD_NAME = "createTopics";
    private static final String OPTIONS_TYPE_TOKEN =
            "Lorg/apache/kafka/clients/admin/CreateTopicsOptions;";

    private final Severity severity;

    public AdminCreateTopicsNoOptionsRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_CREATE_TOPICS_NO_OPTIONS;
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
                RuleId.ADMIN_CREATE_TOPICS_NO_OPTIONS, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Admin.createTopics(Collection<NewTopic>) — a "
                        + "no-CreateTopicsOptions overload is reached "
                        + "here — either as a direct INVOKEINTERFACE on "
                        + "Admin / INVOKEVIRTUAL on AdminClient or as "
                        + "an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `admin::createTopics` bound to "
                        + "Function<Collection, CreateTopicsResult> or "
                        + "to a custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). "
                        + "Without an explicit CreateTopicsOptions the "
                        + "call uses request.timeout.ms (default ~30s) "
                        + "as the deadline for the controller to ack; "
                        + "when the future times out the caller sees a "
                        + "TimeoutException — but that exception does "
                        + "NOT mean the topic was not created. The "
                        + "controller may still be processing the "
                        + "request; the topic may appear on the cluster "
                        + "seconds after the timeout fires. Concrete "
                        + "failure modes: (1) cluster bring-up scripts "
                        + "double-create on retry — bootstrap script "
                        + "calls admin.createTopics(List.of(new "
                        + "NewTopic(\"orders\", 12, (short) 3))) as "
                        + "part of provisioning; controller is under "
                        + "load (large rebalance in flight); the call "
                        + "times out at 30s; the script catches "
                        + "TimeoutException and retries with the same "
                        + "topic spec; the second call observes "
                        + "TopicExistsException because the first "
                        + "request actually succeeded after the "
                        + "timeout; the retry logic was not written to "
                        + "expect that race; the bootstrap fails; SRE "
                        + "rolls back the deploy and the cluster is "
                        + "left half-provisioned with one topic "
                        + "created and zero ACLs applied; (2) mid-"
                        + "incident topic-creation deadlines exceed "
                        + "oncall runbook time-boxes — during a "
                        + "production incident an SRE creates a "
                        + "quarantine topic to redirect a poison-pill "
                        + "stream; the runbook says \"create the "
                        + "quarantine topic and confirm within 90 "
                        + "seconds, otherwise escalate to L2\"; the "
                        + "createTopics() call times out at 30s with "
                        + "no way for the SRE to extend the bound "
                        + "from the call site; they can't tell "
                        + "whether the topic was created or not (the "
                        + "future is failed; checking with "
                        + "describeTopics adds another 30s); they "
                        + "escalate; L2 arrives to find the topic was "
                        + "created 15s before they picked up the "
                        + "page; (3) CI/CD topic-provisioning jobs "
                        + "flake under controller load — CI provisions "
                        + "a per-PR namespace of topics via a single "
                        + "createTopics call; as the test cluster "
                        + "scales (more PRs in flight = more "
                        + "controller contention), the default 30s "
                        + "timeout starts catching the tail of "
                        + "legitimate operations; the CI step fails "
                        + "intermittently with TimeoutException; "
                        + "engineers click \"retry\" until it passes; "
                        + "the apparent flake distribution hides a "
                        + "real cluster-load problem until the "
                        + "controller falls over; (4) operator can't "
                        + "bound the operation from outside — without "
                        + "CreateTopicsOptions.timeoutMs(long) on the "
                        + "call, the deadline is determined by "
                        + "whatever request.timeout.ms the AdminClient "
                        + "was configured with at construction time, "
                        + "typically global config baked into a shared "
                        + "library; tuning per-call deadlines requires "
                        + "editing the AdminClient construction (and "
                        + "any caching it sits behind) rather than the "
                        + "call site; the caller has no local control "
                        + "over the operation deadline; (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "`Function<Collection<NewTopic>, "
                        + "CreateTopicsResult> provisioner = "
                        + "admin::createTopics` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "Admin.createTopics(Collection)"
                        + "CreateTopicsResult; the user-class bytecode "
                        + "contains zero direct INVOKEINTERFACE on the "
                        + "no-Options overload, only the indy site. "
                        + "Migration: pass an explicit "
                        + "CreateTopicsOptions pinning a per-call "
                        + "deadline — `admin.createTopics(List.of(new "
                        + "NewTopic(\"orders\", 12, (short) 3)), new "
                        + "CreateTopicsOptions().timeoutMs(60_000))`. "
                        + "The bound is local to the call site and "
                        + "explicitly chosen, so retry logic upstream "
                        + "can distinguish \"in flight\" from "
                        + "\"failed\" by comparing against the chosen "
                        + "deadline. The createTopics(Collection, "
                        + "CreateTopicsOptions) overload is never "
                        + "flagged by this rule.");
    }
}
