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
 * {@link org.apache.kafka.clients.admin.Admin#deleteTopics(java.util.Collection)
 * Admin.deleteTopics(Collection)} or
 * {@link org.apache.kafka.clients.admin.Admin#deleteTopics(
 * org.apache.kafka.clients.admin.TopicCollection)
 * Admin.deleteTopics(TopicCollection)} overload that does NOT
 * include a {@link org.apache.kafka.clients.admin.DeleteTopicsOptions
 * DeleteTopicsOptions} argument and therefore falls back to the
 * default {@code request.timeout.ms} (~30 s) with no caller-
 * visible bound on the destructive operation.
 *
 * <p>Predicate is structural — any descriptor for {@code
 * deleteTopics} on {@code Admin} or {@code AdminClient} whose
 * argument list contains
 * {@code org/apache/kafka/clients/admin/DeleteTopicsOptions} is
 * safe; any descriptor that does not is unsafe. Covers both
 * unsafe overload pairs:
 *
 * <ul>
 *   <li>{@code deleteTopics(Collection<String>)} vs
 *       {@code deleteTopics(Collection<String>, DeleteTopicsOptions)}</li>
 *   <li>{@code deleteTopics(TopicCollection)} vs
 *       {@code deleteTopics(TopicCollection, DeleteTopicsOptions)}</li>
 * </ul>
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls on {@link
 * org.apache.kafka.clients.admin.Admin Admin} (the interface),
 * {@code INVOKEVIRTUAL} calls on {@link
 * org.apache.kafka.clients.admin.AdminClient AdminClient} (the
 * abstract class), and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code admin::deleteTopics} bound to {@link
 * java.util.function.Function
 * Function&lt;Collection, DeleteTopicsResult&gt;}).
 *
 * <h2>Why no-Options deleteTopics is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.admin.Admin#deleteTopics(
 * java.util.Collection) Admin.deleteTopics} is asynchronous AND
 * DESTRUCTIVE: it issues a DeleteTopics request to the
 * controller and returns a future. Without an explicit {@link
 * org.apache.kafka.clients.admin.DeleteTopicsOptions}, the call
 * uses {@code request.timeout.ms} (default ~30 s) as the
 * deadline. When the future times out the caller sees a {@code
 * TimeoutException} — but that exception does NOT mean the
 * topic was not deleted. The controller may still be processing
 * the request; the topic may disappear from the cluster seconds
 * after the timeout fires. The caller has no way to distinguish
 * "request never reached the controller" from "request reached
 * the controller, controller is still working on it" from
 * "topic was deleted seconds ago, all its data is gone".
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Cleanup scripts re-delete the wrong topic after the
 *       first call times out.</b> A nightly cleanup job calls
 *       {@code admin.deleteTopics(List.of("temp-2024-12-31"))}
 *       and times out at 30 s. The script's retry logic
 *       interprets {@code TimeoutException} as "delete failed,
 *       try again later". Two days later a SIMILAR topic name
 *       {@code "temp-2025-01-02"} exists; the original delete
 *       actually succeeded after timeout, but the retry now
 *       targets a different list of topics chosen by the next
 *       cleanup pass — and the script's idempotency assumption
 *       (delete is safe to retry) silently means an extra
 *       topic is deleted that the original request would
 *       never have touched.</li>
 *   <li><b>Disaster-recovery runbooks misread the operation
 *       state.</b> During an incident an oncall runs {@code
 *       admin.deleteTopics(List.of("corrupted-topic"))} to
 *       remove a poison topic and restore the streaming
 *       pipeline. The call times out at 30 s; oncall assumes
 *       failure and creates a JIRA "delete didn't work, manual
 *       intervention needed"; a teammate sees the topic is
 *       already gone moments later and closes the ticket as
 *       "no repro"; the audit trail loses the actual
 *       sequence of events; post-mortem authors can't tell
 *       whether the delete was the human's intent or a
 *       follow-on cluster behaviour.</li>
 *   <li><b>Test harnesses leak topics under controller load.</b>
 *       An integration test suite creates per-test topics and
 *       deletes them in {@code @AfterEach}. Under CI load the
 *       30 s deadline catches the tail of legitimate deletes;
 *       the harness logs warnings ("delete timed out, will
 *       retry next run") and the test moves on; over weeks the
 *       cluster accumulates thousands of leaked test topics
 *       (deletes that did succeed past the timeout never get
 *       cleaned up because the harness believes they failed
 *       and is waiting for the NEXT cleanup cycle).</li>
 *   <li><b>Operator can't bound the operation from outside.</b>
 *       Without {@code DeleteTopicsOptions.timeoutMs(long)} on
 *       the call, the deadline is determined by whatever
 *       {@code request.timeout.ms} the AdminClient was
 *       configured with at construction time — typically
 *       global config baked into a shared library. The caller
 *       has no local control over the destructive-operation
 *       deadline. Tuning per-call deadlines for "expensive"
 *       deletes (large partition counts, replication-factor 3,
 *       full broker disks under deletion) requires editing the
 *       AdminClient construction rather than the call site.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       cleanup utility built as {@code Function&lt;Collection
 *       &lt;String&gt;, DeleteTopicsResult&gt; deleter =
 *       admin::deleteTopics} compiles to {@code INVOKEDYNAMIC}
 *       whose bsm-args contain a {@code REF_invokeInterface}
 *       Handle pointing at {@code Admin.deleteTopics(Collection)
 *       DeleteTopicsResult}. The user-class bytecode contains
 *       zero direct {@code INVOKEINTERFACE} on the no-Options
 *       overload, only the indy site.</li>
 * </ul>
 *
 * <p>Migration: pass an explicit {@code DeleteTopicsOptions}
 * pinning a per-call deadline — e.g. {@code admin.deleteTopics(
 * List.of("orders"), new DeleteTopicsOptions().timeoutMs(
 * 60_000))}. The bound is local to the call site and explicitly
 * chosen, so retry logic upstream can distinguish "in flight"
 * from "failed" by comparing against the chosen deadline before
 * issuing a follow-up destructive operation.
 */
public final class AdminDeleteTopicsNoOptionsRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.ADMIN_OWNERS;
    private static final String METHOD_NAME = "deleteTopics";
    private static final String OPTIONS_TYPE_TOKEN =
            "Lorg/apache/kafka/clients/admin/DeleteTopicsOptions;";

    private final Severity severity;

    public AdminDeleteTopicsNoOptionsRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_DELETE_TOPICS_NO_OPTIONS;
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
                RuleId.ADMIN_DELETE_TOPICS_NO_OPTIONS, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Admin.deleteTopics(Collection<String>) / "
                        + "deleteTopics(TopicCollection) — a "
                        + "no-DeleteTopicsOptions overload is reached "
                        + "here — either as a direct INVOKEINTERFACE "
                        + "on Admin / INVOKEVIRTUAL on AdminClient or "
                        + "as an INVOKEDYNAMIC method-reference "
                        + "capture (e.g. `admin::deleteTopics` bound "
                        + "to Function<Collection, DeleteTopicsResult> "
                        + "or to a custom SAM whose erased implMethod "
                        + "descriptor matches an unsafe overload). "
                        + "deleteTopics is asynchronous AND "
                        + "DESTRUCTIVE; without an explicit "
                        + "DeleteTopicsOptions the call uses "
                        + "request.timeout.ms (default ~30s) as the "
                        + "deadline; when the future times out the "
                        + "caller sees a TimeoutException — but that "
                        + "exception does NOT mean the topic was not "
                        + "deleted. The controller may still be "
                        + "processing the request; the topic may "
                        + "disappear from the cluster seconds after "
                        + "the timeout fires. Concrete failure modes: "
                        + "(1) cleanup scripts re-delete the wrong "
                        + "topic after the first call times out — a "
                        + "nightly cleanup job calls "
                        + "admin.deleteTopics(List.of(\"temp-2024-12-"
                        + "31\")) and times out at 30s; the script's "
                        + "retry logic interprets TimeoutException as "
                        + "\"delete failed, try again later\"; two "
                        + "days later a SIMILAR topic name \"temp-"
                        + "2025-01-02\" exists; the original delete "
                        + "actually succeeded after timeout, but the "
                        + "retry now targets a different list of "
                        + "topics chosen by the next cleanup pass — "
                        + "and the script's idempotency assumption "
                        + "(delete is safe to retry) silently means "
                        + "an extra topic is deleted that the "
                        + "original request would never have touched; "
                        + "(2) disaster-recovery runbooks misread "
                        + "the operation state — during an incident "
                        + "an oncall runs admin.deleteTopics(List.of("
                        + "\"corrupted-topic\")) to remove a poison "
                        + "topic and restore the streaming pipeline; "
                        + "the call times out at 30s; oncall assumes "
                        + "failure and creates a JIRA \"delete didn't "
                        + "work, manual intervention needed\"; a "
                        + "teammate sees the topic is already gone "
                        + "moments later and closes the ticket as "
                        + "\"no repro\"; the audit trail loses the "
                        + "actual sequence of events; post-mortem "
                        + "authors can't tell whether the delete was "
                        + "the human's intent or a follow-on cluster "
                        + "behaviour; (3) test harnesses leak topics "
                        + "under controller load — an integration "
                        + "test suite creates per-test topics and "
                        + "deletes them in @AfterEach; under CI load "
                        + "the 30s deadline catches the tail of "
                        + "legitimate deletes; the harness logs "
                        + "warnings (\"delete timed out, will retry "
                        + "next run\") and the test moves on; over "
                        + "weeks the cluster accumulates thousands of "
                        + "leaked test topics (deletes that did "
                        + "succeed past the timeout never get cleaned "
                        + "up because the harness believes they "
                        + "failed and is waiting for the NEXT cleanup "
                        + "cycle); (4) operator can't bound the "
                        + "operation from outside — without "
                        + "DeleteTopicsOptions.timeoutMs(long) on the "
                        + "call, the deadline is determined by "
                        + "whatever request.timeout.ms the "
                        + "AdminClient was configured with at "
                        + "construction time, typically global config "
                        + "baked into a shared library; the caller "
                        + "has no local control over the destructive-"
                        + "operation deadline; tuning per-call "
                        + "deadlines for \"expensive\" deletes "
                        + "(large partition counts, replication-"
                        + "factor 3, full broker disks under "
                        + "deletion) requires editing the AdminClient "
                        + "construction rather than the call site; "
                        + "(5) INVOKEDYNAMIC method-reference "
                        + "captures bypass naive MethodInsnNode-only "
                        + "lint — `Function<Collection<String>, "
                        + "DeleteTopicsResult> deleter = "
                        + "admin::deleteTopics` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "Admin.deleteTopics(Collection)"
                        + "DeleteTopicsResult; the user-class "
                        + "bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Options "
                        + "overload, only the indy site. Migration: "
                        + "pass an explicit DeleteTopicsOptions "
                        + "pinning a per-call deadline — "
                        + "`admin.deleteTopics(List.of(\"orders\"), "
                        + "new DeleteTopicsOptions().timeoutMs("
                        + "60_000))`. The bound is local to the call "
                        + "site and explicitly chosen, so retry logic "
                        + "upstream can distinguish \"in flight\" "
                        + "from \"failed\" by comparing against the "
                        + "chosen deadline before issuing a follow-up "
                        + "destructive operation. The "
                        + "deleteTopics(*, DeleteTopicsOptions) "
                        + "overloads are never flagged by this rule.");
    }
}
