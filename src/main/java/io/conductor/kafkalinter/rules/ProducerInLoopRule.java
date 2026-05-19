package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.Opcodes;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class ProducerInLoopRule implements Rule {

    private final Severity severity;
    private final RuleId ruleId;
    private final Set<String> targetTypes;

    public ProducerInLoopRule(RuleId ruleId, Severity severity, Set<String> targetTypes) {
        this.ruleId = ruleId;
        this.severity = severity;
        this.targetTypes = targetTypes;
    }

    @Override
    public RuleId id() {
        return ruleId;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn.getOpcode() != Opcodes.NEW) continue;
                if (!(insn instanceof TypeInsnNode tn)) continue;
                if (!targetTypes.contains(tn.desc)) continue;
                if (!ctx.isInLoopOrIteratingLambda(mn, insn)) continue;

                String location = ctx.isIteratingLambdaBody(mn) ? "iterating lambda" : "loop";
                String detail = "new " + tn.desc.substring(tn.desc.lastIndexOf('/') + 1)
                        + " inside " + location + " — instantiate once and reuse.";
                out.add(new Violation(ruleId, severity, ctx.classNode().name, mn.name, AsmUtil.lineOf(insn), detail));
            }
        }
        return out;
    }
}
