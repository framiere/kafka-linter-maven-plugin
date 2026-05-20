package io.conductor.kafkalinter.rules.config;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Generic "any call to method {@code name} on an owner in {@code owners}" detector.
 *
 * <p>Use this for rules whose entire signal is "this method should not be called from
 * production code" — KafkaStreams.cleanUp(), the deprecated KStream.through(),
 * ObjectMapper.activateDefaultTyping(), and so on.
 */
public final class MethodCallRule implements Rule {

    private final RuleId ruleId;
    private final Severity severity;
    private final Set<String> owners;
    private final Set<String> methodNames;
    private final String detail;

    public MethodCallRule(RuleId ruleId, Severity severity,
                          Set<String> owners, Set<String> methodNames,
                          String detail) {
        this.ruleId = ruleId;
        this.severity = severity;
        this.owners = owners;
        this.methodNames = methodNames;
        this.detail = detail;
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
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!owners.contains(mi.owner)) continue;
                if (!methodNames.contains(mi.name)) continue;
                out.add(new Violation(ruleId, severity, ctx.classNode().name, mn.name,
                        AsmUtil.lineOf(insn), detail));
            }
        }
        return out;
    }
}
