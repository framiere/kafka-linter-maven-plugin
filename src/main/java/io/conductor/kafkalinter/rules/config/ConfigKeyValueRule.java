package io.conductor.kafkalinter.rules.config;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Generic "config key has a literal bad value" detector.
 *
 * <p>Matches the same shape that {@code ConsumerAutoCommitTrueRule} pioneered:
 * find a {@link java.util.Properties#put(Object, Object)} / {@code Map.put(...)} /
 * {@code setProperty(...)} where the immediately-preceding two-LDC pair is the
 * watched key and a value the caller-provided predicate considers "bad".
 *
 * <p>Limitation: matches literal forms only. If the value is computed at runtime
 * or boxed via {@code Boolean.toString}, the rule will not fire — this is by
 * design, to keep false-positives at zero.
 */
public final class ConfigKeyValueRule implements Rule {

    private final RuleId ruleId;
    private final Severity severity;
    private final String watchedKey;
    private final Predicate<String> badValue;
    private final String detailTemplate;

    public ConfigKeyValueRule(RuleId ruleId, Severity severity,
                              String watchedKey, Predicate<String> badValue,
                              String detailTemplate) {
        this.ruleId = ruleId;
        this.severity = severity;
        this.watchedKey = watchedKey;
        this.badValue = badValue;
        this.detailTemplate = detailTemplate;
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
                if (!KafkaTypes.CONFIG_HOLDERS.contains(mi.owner)) continue;
                if (!KafkaTypes.CONFIG_PUT_METHODS.contains(mi.name)) continue;

                AbstractInsnNode valueLdc = AsmUtil.prevSignificant(insn);
                AbstractInsnNode keyLdc = AsmUtil.prevSignificant(valueLdc);
                if (!(keyLdc instanceof LdcInsnNode keyL)) continue;
                if (!(valueLdc instanceof LdcInsnNode valL)) continue;
                if (!watchedKey.equals(keyL.cst)) continue;

                String value = String.valueOf(valL.cst);
                if (!badValue.test(value)) continue;

                String detail = detailTemplate.replace("{value}", value);
                out.add(new Violation(ruleId, severity, ctx.classNode().name, mn.name,
                        AsmUtil.lineOf(insn), detail));
            }
        }
        return out;
    }

    public static ConfigKeyValueRule literal(RuleId id, Severity sev, String key, String badLiteral,
                                             String detailTemplate) {
        return new ConfigKeyValueRule(id, sev, key, badLiteral::equals, detailTemplate);
    }

    public static ConfigKeyValueRule literalAny(RuleId id, Severity sev, String key, Set<String> badLiterals,
                                                String detailTemplate) {
        return new ConfigKeyValueRule(id, sev, key, badLiterals::contains, detailTemplate);
    }
}
