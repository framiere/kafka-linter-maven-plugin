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

/**
 * Flags producer.send(record).get() — the immediate {@code .get()} on the Future
 * returned by send() turns the call synchronous and defeats batching/async.
 */
public final class ProducerSendBlockingGetRule implements Rule {

    private final Severity severity;

    public ProducerSendBlockingGetRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_SEND_BLOCKING_GET;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;
                if (!mi.name.equals("send")) continue;

                AbstractInsnNode next = AsmUtil.nextSignificant(insn);
                if (next instanceof MethodInsnNode getCall
                        && getCall.owner.equals(KafkaTypes.FUTURE)
                        && getCall.name.equals("get")) {
                    out.add(new Violation(
                            RuleId.PRODUCER_SEND_BLOCKING_GET,
                            severity,
                            ctx.classNode().name,
                            mn.name,
                            AsmUtil.lineOf(insn),
                            "send(record).get() is synchronous — use a Callback instead."));
                }
            }
        }
        return out;
    }
}
