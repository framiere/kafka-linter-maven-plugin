package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires when {@code admin.close(Duration.ZERO)} or
 * {@code admin.close(Duration.ofXxx(0))} is called — abandons every in-flight
 * admin request without giving the AdminClient runnable a chance to flush its
 * queue. Pending KafkaFutures complete with TimeoutException, the operations
 * the application thought were in flight may or may not have reached the
 * cluster, and the caller has no signal that work was lost.
 */
public final class AdminCloseZeroDurationRule implements Rule {

    private static final String CLOSE_DURATION_DESC = "(Ljava/time/Duration;)V";
    private static final String DURATION_ZERO_FIELD = "ZERO";
    private static final String DURATION_DESC = "Ljava/time/Duration;";

    private static final Set<String> DURATION_FACTORY_METHODS = Set.of(
            "ofMillis", "ofSeconds", "ofNanos", "ofMinutes", "ofHours", "ofDays"
    );

    private final Severity severity;

    public AdminCloseZeroDurationRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_CLOSE_ZERO_DURATION;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!isAdminCloseDuration(insn)) continue;
                AbstractInsnNode arg = AsmUtil.prevSignificant(insn);
                String shape = zeroDurationShape(arg);
                if (shape == null) continue;

                out.add(new Violation(
                        RuleId.ADMIN_CLOSE_ZERO_DURATION, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "admin.close(" + shape + ") — abandons every in-flight admin request. The "
                                + "AdminClientRunnable event loop gets zero time to drain its queue; every "
                                + "pending KafkaFuture (from createTopics, deleteTopics, alterConfigs, "
                                + "listConsumerGroupOffsets, describeCluster, ...) completes with "
                                + "TimeoutException, but the operation may have already been sent to the "
                                + "broker — the caller cannot tell whether the cluster-side change "
                                + "happened or not. Most damaging in IaC reconcilers, one-shot CLIs and "
                                + "operator-style controllers where the *final* operation in the chain "
                                + "(the verification call after a destructive change) is the one most "
                                + "likely to be in flight at close time. The tool reports a clean exit, "
                                + "the cluster diverges from the intended state, and the gap is invisible "
                                + "until someone audits the cluster manually. Pass a Duration matched to "
                                + "the cluster's operation latency, typically close(Duration.ofSeconds(30))."));
            }
        }
        return out;
    }

    private static boolean isAdminCloseDuration(AbstractInsnNode insn) {
        if (!(insn instanceof MethodInsnNode mi)) return false;
        if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL && mi.getOpcode() != Opcodes.INVOKEINTERFACE) return false;
        if (!KafkaTypes.ADMIN_OWNERS.contains(mi.owner)) return false;
        if (!"close".equals(mi.name)) return false;
        return CLOSE_DURATION_DESC.equals(mi.desc);
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
            if (AsmUtil.isLongZeroLiteral(literal)) {
                return "Duration." + mi.name + "(0)";
            }
        }
        return null;
    }
}
