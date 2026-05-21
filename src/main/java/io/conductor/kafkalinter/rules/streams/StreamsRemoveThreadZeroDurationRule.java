package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires when {@code KafkaStreams.removeStreamThread(Duration.ZERO)} or
 * {@code removeStreamThread(Duration.ofXxx(0))} is called — force-removes the
 * StreamThread without giving it a chance to flush state stores, commit
 * offsets, or abort the open EOS transaction.
 *
 * <p>The matched descriptor is
 * {@code (Ljava/time/Duration;)Ljava/util/Optional;} — the bounded
 * {@code removeStreamThread(Duration)} overload. The no-argument
 * {@code removeStreamThread()} ({@code ()Ljava/util/Optional;}) is covered
 * by [[streams-remove-thread-no-timeout]].
 */
public final class StreamsRemoveThreadZeroDurationRule implements Rule {

    private static final String REMOVE_DURATION_DESC = "(Ljava/time/Duration;)Ljava/util/Optional;";
    private static final String DURATION_ZERO_FIELD = "ZERO";
    private static final String DURATION_DESC = "Ljava/time/Duration;";

    private static final Set<String> DURATION_FACTORY_METHODS = Set.of(
            "ofMillis", "ofSeconds", "ofNanos", "ofMinutes", "ofHours", "ofDays"
    );

    private final Severity severity;

    public StreamsRemoveThreadZeroDurationRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_REMOVE_THREAD_ZERO_DURATION;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!isRemoveThreadDuration(insn)) continue;
                AbstractInsnNode arg = AsmUtil.prevSignificant(insn);
                String shape = zeroDurationShape(arg);
                if (shape == null) continue;

                out.add(new Violation(
                        RuleId.STREAMS_REMOVE_THREAD_ZERO_DURATION, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "KafkaStreams.removeStreamThread(" + shape + ") — force-removes the StreamThread "
                                + "without waiting for it to leave PENDING_SHUTDOWN. The thread is interrupted "
                                + "mid-process() so the tasks it owns leave behind unflushed state-store caches, "
                                + "uncommitted source offsets and (for processing.guarantee=exactly_once_v2) an "
                                + "abandoned transaction. After the next rebalance the surviving threads pick up "
                                + "those tasks and replay records from the last committed offset — duplicate "
                                + "processing on every scale-down. For EOS-v2 the reassignee hits "
                                + "ProducerFencedException on its first beginTransaction() until the broker times "
                                + "out the orphaned transaction (transaction.timeout.ms, default 60 s); the task "
                                + "is effectively offline for that window — lag accumulates and downstream "
                                + "consumers see a gap. Pass a Duration matched to the caller's deadline (e.g. "
                                + "the HTTP request budget, the autoscaler's reconciliation interval) and handle "
                                + "Optional.empty() as 'thread did not stop in time' — retry next cycle or "
                                + "escalate, do not assume the thread is gone."));
            }
        }
        return out;
    }

    private static boolean isRemoveThreadDuration(AbstractInsnNode insn) {
        if (!(insn instanceof MethodInsnNode mi)) return false;
        if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL) return false;
        if (!KafkaTypes.KAFKA_STREAMS.equals(mi.owner)) return false;
        if (!"removeStreamThread".equals(mi.name)) return false;
        return REMOVE_DURATION_DESC.equals(mi.desc);
    }

    /**
     * Returns a human-readable shape string (e.g. "Duration.ZERO", "Duration.ofMillis(0)")
     * if {@code arg} represents a zero Duration, otherwise null.
     */
    private static String zeroDurationShape(AbstractInsnNode arg) {
        if (arg instanceof FieldInsnNode f
                && f.getOpcode() == Opcodes.GETSTATIC
                && KafkaTypes.DURATION.equals(f.owner)
                && DURATION_ZERO_FIELD.equals(f.name)
                && DURATION_DESC.equals(f.desc)) {
            return "Duration.ZERO";
        }
        if (arg instanceof MethodInsnNode mi
                && mi.getOpcode() == Opcodes.INVOKESTATIC
                && KafkaTypes.DURATION.equals(mi.owner)
                && DURATION_FACTORY_METHODS.contains(mi.name)) {
            AbstractInsnNode literal = AsmUtil.prevSignificant(mi);
            if (isLongZeroLiteral(literal)) {
                return "Duration." + mi.name + "(0)";
            }
        }
        return null;
    }

    private static boolean isLongZeroLiteral(AbstractInsnNode insn) {
        if (insn == null) return false;
        if (insn.getOpcode() == Opcodes.LCONST_0) return true;
        if (insn instanceof LdcInsnNode ldc && ldc.cst instanceof Long l && l == 0L) return true;
        if (insn.getOpcode() == Opcodes.I2L) {
            AbstractInsnNode prev = AsmUtil.prevSignificant(insn);
            if (prev == null) return false;
            return prev.getOpcode() == Opcodes.ICONST_0
                    || (prev instanceof LdcInsnNode ldc && ldc.cst instanceof Integer i && i == 0)
                    || (prev instanceof InsnNode && prev.getOpcode() == Opcodes.ICONST_0);
        }
        return false;
    }
}
