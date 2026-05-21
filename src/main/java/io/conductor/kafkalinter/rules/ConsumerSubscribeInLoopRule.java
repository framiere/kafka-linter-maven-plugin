package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires when {@code consumer.subscribe(...)} or {@code consumer.assign(...)}
 * is called inside a loop body (or iterating-lambda body).
 *
 * <p>Both methods are lifecycle setup calls. Calling them inside a loop
 * forces a coordinator-side rebalance (subscribe) or assignment reset
 * (assign) on every iteration — the group never converges, lag grows
 * monotonically, and the broker rebalance throttle starts kicking in.
 */
public final class ConsumerSubscribeInLoopRule implements Rule {

    private static final Set<String> RESET_METHODS = Set.of("subscribe", "assign");

    private final Severity severity;

    public ConsumerSubscribeInLoopRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_SUBSCRIBE_IN_LOOP;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if (!RESET_METHODS.contains(mi.name)) continue;
                if (!ctx.isInLoopOrIteratingLambda(mn, insn)) continue;

                String where = ctx.isIteratingLambdaBody(mn) ? "an iterating lambda" : "a loop";
                out.add(new Violation(
                        RuleId.CONSUMER_SUBSCRIBE_IN_LOOP, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "consumer." + mi.name + "(...) inside " + where
                                + " — every iteration resets the consumer's subscription state and "
                                + "schedules a group rebalance (or assignment reset). The group never "
                                + "converges, lag grows monotonically, and the broker rebalance throttle "
                                + "starts kicking in. Call subscribe()/assign() exactly once during "
                                + "consumer setup, before the first poll() — never inside the poll loop "
                                + "or a per-request helper."));
            }
        }
        return out;
    }
}
