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
 * Fires for every {@code Consumer.close()} call with the no-argument
 * descriptor. The bounded overload {@code close(Duration)} enforces a
 * wall-clock deadline; the no-arg variant delegates to
 * {@code close(Duration.ofMillis(defaultApiTimeoutMs))} (60 s default) and
 * blocks while it commits final offsets, sends the LeaveGroup, and closes
 * broker connections.
 *
 * <p>The rule matches calls whose method name is {@code close} and whose
 * descriptor is {@code ()V}, on either the {@code Consumer} interface or
 * the {@code KafkaConsumer} concrete class. The bounded overload
 * {@code close(Duration)} (descriptor {@code (Ljava/time/Duration;)V}) is
 * intentionally not flagged.
 *
 * <p>Method references like {@code consumer::close} ARE flagged. javac
 * compiles {@code new Thread(consumer::close)} to an {@code INVOKEDYNAMIC}
 * whose bootstrap-method arguments include a direct
 * {@code REF_invokeVirtual KafkaConsumer.close:()V} handle — no
 * {@code lambda$N} body, no {@code INVOKEVIRTUAL} in the outer method. The
 * deferred call has the same effect as a direct no-arg close.
 */
public final class ConsumerCloseNoTimeoutRule implements Rule {

    private static final String CLOSE = "close";
    private static final String CLOSE_NO_ARG_DESC = "()V";

    private final Severity severity;

    public ConsumerCloseNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_CLOSE_NO_TIMEOUT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)
                        && CLOSE.equals(mi.name)
                        && CLOSE_NO_ARG_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, KafkaTypes.CONSUMER_OWNERS, CLOSE, CLOSE_NO_ARG_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.CONSUMER_CLOSE_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.close() (no-argument overload) is reached here — either as a direct "
                        + "call or as a method-reference capture (e.g. `consumer::close`) whose "
                        + "deferred invocation has the same effect. Internally "
                        + "this delegates to close(Duration.ofMillis(defaultApiTimeoutMs)) — "
                        + "default.api.timeout.ms is 60 s. The close sequence has to "
                        + "(a) commit final offsets if auto-commit is on, (b) send a "
                        + "LeaveGroup request to the coordinator so the group rebalances "
                        + "promptly, and (c) close every broker connection. Each step has its "
                        + "own retry budget. Against an unhealthy coordinator (failover, GC "
                        + "pause, network partition), each retry loop runs to its budget, "
                        + "summing up to the overall 60 s deadline — much longer than the "
                        + "default Kubernetes terminationGracePeriodSeconds of 30 s. When "
                        + "Kubernetes sends SIGKILL at the end of the grace period, the "
                        + "LeaveGroup never arrives, the remaining consumers wait "
                        + "session.timeout.ms (default 45 s) for the heartbeat slot to expire "
                        + "before the rebalance triggers, and the group's lag spikes for "
                        + "nearly a minute. Use the bounded overload close(Duration). Pick a "
                        + "timeout safely below terminationGracePeriodSeconds (e.g. "
                        + "Duration.ofSeconds(20) for the default 30 s grace) so the "
                        + "LeaveGroup gets a real chance to arrive and the rebalance triggers "
                        + "without waiting on session.timeout.ms.");
    }
}
