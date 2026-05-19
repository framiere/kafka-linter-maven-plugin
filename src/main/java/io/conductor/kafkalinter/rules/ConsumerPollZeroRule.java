package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags consumer.poll(0L) and consumer.poll(Duration.ZERO) — both busy-loop the consumer.
 *
 * Forms detected:
 *   - poll(J)  preceded by LCONST_0          → poll(0L)
 *   - poll(Duration) preceded by GETSTATIC Duration.ZERO  → poll(Duration.ZERO)
 *   - poll(Duration) preceded by INVOKESTATIC Duration.ofMillis(0)/ofNanos(0)  → handled
 */
public final class ConsumerPollZeroRule implements Rule {

    private final Severity severity;

    public ConsumerPollZeroRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_POLL_ZERO;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if (!mi.name.equals("poll")) continue;

                Type[] args = Type.getArgumentTypes(mi.desc);
                if (args.length != 1) continue;

                AbstractInsnNode prev = AsmUtil.prevSignificant(insn);
                if (isPollZero(prev, args[0])) {
                    out.add(new Violation(
                            RuleId.CONSUMER_POLL_ZERO,
                            severity,
                            ctx.classNode().name,
                            mn.name,
                            AsmUtil.lineOf(insn),
                            "poll() called with zero timeout — busy-loops the consumer."));
                }
            }
        }
        return out;
    }

    private static boolean isPollZero(AbstractInsnNode prev, Type argType) {
        if (prev == null) return false;

        if (argType.getSort() == Type.LONG) {
            return prev.getOpcode() == Opcodes.LCONST_0;
        }
        if (argType.getInternalName().equals(KafkaTypes.DURATION)) {
            if (prev instanceof FieldInsnNode f
                    && f.getOpcode() == Opcodes.GETSTATIC
                    && f.owner.equals(KafkaTypes.DURATION)
                    && f.name.equals("ZERO")) {
                return true;
            }
            // Duration.ofMillis(0) / ofNanos(0) / ofSeconds(0)
            if (prev instanceof MethodInsnNode m
                    && m.getOpcode() == Opcodes.INVOKESTATIC
                    && m.owner.equals(KafkaTypes.DURATION)
                    && (m.name.equals("ofMillis") || m.name.equals("ofNanos") || m.name.equals("ofSeconds"))) {
                AbstractInsnNode beforeOf = AsmUtil.prevSignificant(m);
                return beforeOf != null && beforeOf.getOpcode() == Opcodes.LCONST_0;
            }
        }
        return false;
    }
}
