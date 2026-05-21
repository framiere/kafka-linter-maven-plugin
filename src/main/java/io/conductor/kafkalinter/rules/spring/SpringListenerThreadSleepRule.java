package io.conductor.kafkalinter.rules.spring;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags {@code @KafkaListener} methods that call {@code Thread.sleep(...)} or
 * {@code TimeUnit.sleep(...)} in their body.
 *
 * <p>The Spring Kafka container drives {@code consumer.poll()} on the same
 * thread it uses to invoke the listener method. {@code Thread.sleep} parks
 * that thread; while it sleeps no records are processed and {@code poll()}
 * does not advance {@code lastPolledTimestamp}. If the sleep plus the rest
 * of the per-batch processing exceeds {@code max.poll.interval.ms} (default
 * 5 min) the group coordinator fences the consumer out, triggering a
 * rebalance and the next consumer to take over hits the same backlog —
 * a thundering-herd rebalance loop.
 *
 * <p>Detection: one {@link Violation} per {@code Thread.sleep} /
 * {@code TimeUnit.sleep} call site inside a {@code @KafkaListener}-annotated
 * method body.
 */
public final class SpringListenerThreadSleepRule implements Rule {

    private static final String THREAD = "java/lang/Thread";
    private static final String TIME_UNIT = "java/util/concurrent/TimeUnit";
    private static final String SLEEP = "sleep";

    private final Severity severity;

    public SpringListenerThreadSleepRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SPRING_LISTENER_THREAD_SLEEP;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            if (!isKafkaListener(mn)) continue;
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!isSleepCall(mi)) continue;
                out.add(new Violation(
                        RuleId.SPRING_LISTENER_THREAD_SLEEP, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "@KafkaListener body calls " + mi.owner + "." + mi.name
                                + "(...) — parks the consumer thread. Spring's listener container drives "
                                + "consumer.poll() on this same thread; while it sleeps, lastPolledTimestamp "
                                + "does not advance and max.poll.interval.ms (default 5 min) can be exceeded "
                                + "by a slow batch, fencing the consumer out of the group and triggering a "
                                + "rebalance cascade. The right shape is back-pressure (pause the container "
                                + "via KafkaListenerEndpointRegistry, route slow work to an internal job "
                                + "queue, or circuit-break the downstream call) — never block the listener "
                                + "thread."));
            }
        }
        return out;
    }

    private static boolean isSleepCall(MethodInsnNode mi) {
        if (!SLEEP.equals(mi.name)) return false;
        return THREAD.equals(mi.owner) || TIME_UNIT.equals(mi.owner);
    }

    private static boolean isKafkaListener(MethodNode mn) {
        return contains(mn.visibleAnnotations, KafkaTypes.SPRING_KAFKA_LISTENER_ANNOTATION)
                || contains(mn.invisibleAnnotations, KafkaTypes.SPRING_KAFKA_LISTENER_ANNOTATION);
    }

    private static boolean contains(List<AnnotationNode> annots, String desc) {
        if (annots == null) return false;
        for (AnnotationNode a : annots) {
            if (desc.equals(a.desc)) return true;
        }
        return false;
    }
}
