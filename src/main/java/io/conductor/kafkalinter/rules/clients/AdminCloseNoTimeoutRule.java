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

/**
 * Fires for every {@code Admin.close()} / {@code AdminClient.close()} call
 * with the no-argument descriptor. The bounded overload
 * {@code close(Duration)} enforces a wall-clock deadline; the no-arg
 * variant delegates to {@code close(Duration.ofMillis(Long.MAX_VALUE))}
 * and waits indefinitely for every outstanding {@code KafkaFuture} to be
 * either completed or failed.
 *
 * <p>The rule matches calls whose method name is {@code close} and whose
 * descriptor is {@code ()V}, on either the {@code Admin} interface or the
 * {@code AdminClient} abstract class. The bounded overload
 * {@code close(Duration)} (descriptor {@code (Ljava/time/Duration;)V}) is
 * intentionally not flagged.
 *
 * <p>Method references like {@code admin::close} ARE flagged. javac compiles
 * {@code new Thread(admin::close)} to an {@code INVOKEDYNAMIC} whose
 * bootstrap-method arguments include a direct
 * {@code REF_invokeInterface Admin.close:()V} handle — no
 * {@code lambda$N} body, no {@code INVOKEINTERFACE} in the outer method.
 * The deferred call has the same effect as a direct no-arg close.
 */
public final class AdminCloseNoTimeoutRule implements Rule {

    private static final String CLOSE = "close";
    private static final String CLOSE_NO_ARG_DESC = "()V";

    private final Severity severity;

    public AdminCloseNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_CLOSE_NO_TIMEOUT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && KafkaTypes.ADMIN_OWNERS.contains(mi.owner)
                        && CLOSE.equals(mi.name)
                        && CLOSE_NO_ARG_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, KafkaTypes.ADMIN_OWNERS, CLOSE, CLOSE_NO_ARG_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.ADMIN_CLOSE_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Admin.close() (no-argument overload) is reached here — either as a direct "
                        + "call or as a method-reference capture (e.g. `admin::close`) whose "
                        + "deferred invocation has the same effect. Internally "
                        + "this delegates to close(Duration.ofMillis(Long.MAX_VALUE)) — the "
                        + "AdminClient transitions to CLOSING (rejecting new operations) and "
                        + "waits for the internal AdminClientRunnable thread to flush every "
                        + "queued and in-flight request. Each operation has its own "
                        + "request.timeout.ms and retry logic; against an unhealthy cluster "
                        + "(controller in election, slow broker, network partition) those "
                        + "futures never resolve and close() hangs forever. The blast radius "
                        + "lands squarely on operational tooling: lag monitors, topic "
                        + "provisioners, IaC reconcilers, Helm pre-delete hooks, Terraform "
                        + "Kafka providers, and 'reset offsets' admin scripts that loop "
                        + "AdminClient operations and call close() at the end. These tools "
                        + "are precisely the ones that need to complete promptly during a "
                        + "cluster incident — exactly when the no-Duration close() will hang. "
                        + "Use the bounded overload close(Duration). For one-shot tools 30 s "
                        + "is usually generous; for long-running operators match the "
                        + "orchestration framework's grace period. On timeout, log/alert and "
                        + "surface the failure instead of letting the tool silently park.");
    }
}
