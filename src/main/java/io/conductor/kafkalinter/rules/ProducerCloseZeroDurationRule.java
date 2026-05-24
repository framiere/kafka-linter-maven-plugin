package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Fires when {@code producer.close(Duration.ZERO)} (or {@code close(Duration.ofXxx(0))})
 * is called — silent data loss for every record still in the accumulator or in flight.
 */
public final class ProducerCloseZeroDurationRule implements Rule {

    private static final String CLOSE_DURATION_DESC = "(Ljava/time/Duration;)V";

    private final Severity severity;

    public ProducerCloseZeroDurationRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_CLOSE_ZERO_DURATION;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!isProducerCloseDuration(insn)) continue;
                AbstractInsnNode arg = AsmUtil.prevSignificant(insn);
                String shape = AsmUtil.zeroDurationLiteralShape(arg);
                if (shape == null) continue;

                out.add(new Violation(
                        RuleId.PRODUCER_CLOSE_ZERO_DURATION, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "producer.close(" + shape + ") — forces immediate shutdown without draining the "
                                + "accumulator. Every record still buffered or in flight is dropped without "
                                + "the registered callback ever firing — silent data loss with no diagnostic "
                                + "trail. For transactional producers, the transactional.id stays fenced on the "
                                + "broker until transaction.timeout.ms elapses, blocking the next instance. "
                                + "Call producer.flush() first if you need to drain pending records, then "
                                + "close(Duration.ofSeconds(30)) (or a value matched to your shutdown budget)."));
            }
        }
        return out;
    }

    private static boolean isProducerCloseDuration(AbstractInsnNode insn) {
        if (!(insn instanceof MethodInsnNode mi)) return false;
        if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL && mi.getOpcode() != Opcodes.INVOKEINTERFACE) return false;
        if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) return false;
        if (!"close".equals(mi.name)) return false;
        return CLOSE_DURATION_DESC.equals(mi.desc);
    }
}
