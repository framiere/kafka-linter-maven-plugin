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
 * Fires when {@code consumer.close(Duration.ZERO)} or
 * {@code consumer.close(Duration.ofXxx(0))} is called — skips the final
 * commit and onPartitionsRevoked broadcast, causes duplicate processing
 * and a session-timeout-long lag spike on every shutdown.
 */
public final class ConsumerCloseZeroDurationRule implements Rule {

    private static final String CLOSE_DURATION_DESC = "(Ljava/time/Duration;)V";

    private final Severity severity;

    public ConsumerCloseZeroDurationRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_CLOSE_ZERO_DURATION;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!isConsumerCloseDuration(insn)) continue;
                AbstractInsnNode arg = AsmUtil.prevSignificant(insn);
                String shape = AsmUtil.zeroDurationLiteralShape(arg);
                if (shape == null) continue;

                out.add(new Violation(
                        RuleId.CONSUMER_CLOSE_ZERO_DURATION, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "consumer.close(" + shape + ") — skips the final commit and the partition-revoke "
                                + "broadcast. Uncommitted offsets are replayed by the next consumer (duplicate "
                                + "processing on every shutdown), ConsumerRebalanceListener.onPartitionsRevoked() "
                                + "never fires (state never flushed), and the group cannot rebalance until "
                                + "session.timeout.ms elapses (default 45s) — partitions go unprocessed for the "
                                + "whole window, lag spikes. Pass a Duration matched to your processing tail, "
                                + "e.g. close(Duration.ofSeconds(10))."));
            }
        }
        return out;
    }

    private static boolean isConsumerCloseDuration(AbstractInsnNode insn) {
        if (!(insn instanceof MethodInsnNode mi)) return false;
        if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL && mi.getOpcode() != Opcodes.INVOKEINTERFACE) return false;
        if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) return false;
        if (!"close".equals(mi.name)) return false;
        return CLOSE_DURATION_DESC.equals(mi.desc);
    }
}
