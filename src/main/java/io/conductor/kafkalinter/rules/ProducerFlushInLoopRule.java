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

public final class ProducerFlushInLoopRule implements Rule {

    private final Severity severity;

    public ProducerFlushInLoopRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_FLUSH_IN_LOOP;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;
                if (!mi.name.equals("flush")) continue;
                if (!ctx.isInLoopOrIteratingLambda(mn, insn)) continue;

                out.add(new Violation(
                        RuleId.PRODUCER_FLUSH_IN_LOOP,
                        severity,
                        ctx.classNode().name,
                        mn.name,
                        AsmUtil.lineOf(insn),
                        "producer.flush() inside a loop defeats batching — call it once before close()."));
            }
        }
        return out;
    }
}
