package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
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

/**
 * Detects a Properties.put / setProperty / Map.put call whose two immediately-preceding
 * arguments are the literal strings "enable.auto.commit" and "true".
 *
 * Limitation: only catches the literal form. If the value is computed or boxed via
 * Boolean.toString, this won't fire. That's deliberate — we want zero false positives
 * on this one, since it's noisy if it ever screams wrongly.
 */
public final class ConsumerAutoCommitTrueRule implements Rule {

    private static final Set<String> PUT_OWNERS = Set.of(
        KafkaTypes.PROPERTIES, KafkaTypes.MAP, KafkaTypes.HASHMAP
    );
    private static final Set<String> PUT_METHODS = Set.of("put", "setProperty");

    private final Severity severity;

    public ConsumerAutoCommitTrueRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_AUTO_COMMIT_TRUE;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!PUT_OWNERS.contains(mi.owner)) continue;
                if (!PUT_METHODS.contains(mi.name)) continue;

                AbstractInsnNode valueLdc = AsmUtil.prevSignificant(insn);
                AbstractInsnNode keyLdc = AsmUtil.prevSignificant(valueLdc);
                if (!(keyLdc instanceof LdcInsnNode keyL)) continue;
                if (!(valueLdc instanceof LdcInsnNode valL)) continue;
                if (!KafkaTypes.ENABLE_AUTO_COMMIT_KEY.equals(keyL.cst)) continue;
                if (!"true".equals(valL.cst)) continue;

                out.add(new Violation(
                        RuleId.CONSUMER_AUTO_COMMIT_TRUE,
                        severity,
                        ctx.classNode().name,
                        mn.name,
                        AsmUtil.lineOf(insn),
                        "enable.auto.commit is set to \"true\" — risks message loss/double-processing on rebalance."));
            }
        }
        return out;
    }
}
