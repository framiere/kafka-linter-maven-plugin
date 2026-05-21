package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires when {@code consumer.poll(Duration.ofXxx(Long.MAX_VALUE))} is called —
 * the poll thread parks for ~292 million years and cannot be unblocked by
 * SIGTERM without an explicit {@code consumer.wakeup()} from a shutdown hook.
 */
public final class ConsumerPollInfiniteDurationRule implements Rule {

    private static final String POLL_DURATION_DESC =
            "(Ljava/time/Duration;)Lorg/apache/kafka/clients/consumer/ConsumerRecords;";

    private static final Set<String> DURATION_FACTORY_METHODS = Set.of(
            "ofMillis", "ofSeconds", "ofNanos", "ofMinutes", "ofHours", "ofDays"
    );

    private final Severity severity;

    public ConsumerPollInfiniteDurationRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_POLL_INFINITE_DURATION;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!isConsumerPollDuration(insn)) continue;
                AbstractInsnNode factory = AsmUtil.prevSignificant(insn);
                if (!isDurationFactoryCall(factory)) continue;
                AbstractInsnNode literal = AsmUtil.prevSignificant(factory);
                if (!isLongMaxValueLdc(literal)) continue;

                String unit = ((MethodInsnNode) factory).name;
                out.add(new Violation(
                        RuleId.CONSUMER_POLL_INFINITE_DURATION, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "consumer.poll(Duration." + unit + "(Long.MAX_VALUE)) — the poll thread parks "
                                + "inside the fetcher's wait for an effectively unbounded interval. "
                                + "SIGTERM cannot unblock it without an explicit consumer.wakeup() from "
                                + "a shutdown hook, so the JVM hangs until the kubelet's grace period "
                                + "expires and SIGKILL fires mid-batch (uncommitted offsets reprocessed). "
                                + "Use a bounded duration (Duration.ofMillis(100-500)) inside a "
                                + "while (!closed) loop, and call consumer.wakeup() from your shutdown hook."));
            }
        }
        return out;
    }

    private static boolean isConsumerPollDuration(AbstractInsnNode insn) {
        if (!(insn instanceof MethodInsnNode mi)) return false;
        if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL && mi.getOpcode() != Opcodes.INVOKEINTERFACE) return false;
        if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) return false;
        if (!"poll".equals(mi.name)) return false;
        return POLL_DURATION_DESC.equals(mi.desc);
    }

    private static boolean isDurationFactoryCall(AbstractInsnNode insn) {
        if (!(insn instanceof MethodInsnNode mi)) return false;
        if (mi.getOpcode() != Opcodes.INVOKESTATIC) return false;
        if (!KafkaTypes.DURATION.equals(mi.owner)) return false;
        return DURATION_FACTORY_METHODS.contains(mi.name);
    }

    private static boolean isLongMaxValueLdc(AbstractInsnNode insn) {
        if (!(insn instanceof LdcInsnNode ldc)) return false;
        return ldc.cst instanceof Long l && l == Long.MAX_VALUE;
    }
}
