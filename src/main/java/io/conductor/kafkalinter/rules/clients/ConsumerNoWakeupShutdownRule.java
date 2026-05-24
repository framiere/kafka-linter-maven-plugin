package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags a {@code KafkaConsumer.poll(...)} inside a loop (or iterating-lambda body) when the
 * class never calls {@code consumer.wakeup()} anywhere. {@code wakeup()} is the only
 * thread-safe interrupt for a blocking {@code poll}; without it the polling thread sleeps
 * up to the poll timeout on JVM shutdown, the consumer never sends LeaveGroup, and the
 * group coordinator holds the partitions for {@code session.timeout.ms}.
 *
 * <p>Suppression rules:
 * <ul>
 *   <li>If any method in the class calls {@code wakeup()} on a Consumer / KafkaConsumer
 *       receiver — either as a direct {@code consumer.wakeup()} invocation or as a
 *       method-reference capture (e.g. {@code consumer::wakeup} handed to
 *       {@code new Thread(...)} for a shutdown hook) — the class is considered safe and
 *       no violations are produced. The method-reference form is the canonical Kafka
 *       shutdown idiom and compiles to an {@code INVOKEDYNAMIC} whose bootstrap-method
 *       args carry the {@code REF_invokeVirtual KafkaConsumer.wakeup:()V} handle: there
 *       is no {@code INVOKEVIRTUAL wakeup} in the outer method's bytecode, so the
 *       suppressor must inspect the indy bootstrap args to see it.</li>
 *   <li>If any method in the class is annotated with Spring's {@code @KafkaListener}, the
 *       consumer is framework-managed and the lifecycle is the container's responsibility.</li>
 * </ul>
 */
public final class ConsumerNoWakeupShutdownRule implements Rule {

    private final Severity severity;

    public ConsumerNoWakeupShutdownRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_NO_WAKEUP_SHUTDOWN;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        if (hasAnyWakeup(ctx) || hasAnyKafkaListener(ctx)) {
            return List.of();
        }
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if (!"poll".equals(mi.name)) continue;
                if (!ctx.isInLoopOrIteratingLambda(mn, insn)) continue;
                out.add(new Violation(
                        RuleId.CONSUMER_NO_WAKEUP_SHUTDOWN, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "Consumer.poll() runs inside a loop but this class never calls "
                                + "consumer.wakeup() — the only thread-safe way to interrupt a "
                                + "blocking poll. On JVM shutdown the polling thread keeps "
                                + "sleeping up to the poll timeout, close() cannot run, the "
                                + "consumer never sends LeaveGroup, and the group coordinator "
                                + "holds the partitions for session.timeout.ms (default 45 s in "
                                + "3.0+). Register a shutdown hook that calls consumer.wakeup(), "
                                + "wrap the poll loop in try { ... } catch (WakeupException) {}, "
                                + "and close() the consumer in a finally."));
            }
        }
        return out;
    }

    private static boolean hasAnyWakeup(RuleContext ctx) {
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)
                        && "wakeup".equals(mi.name)) {
                    return true;
                }
                // Method-reference capture: `consumer::wakeup` compiles to an
                // INVOKEDYNAMIC whose bootstrap args contain a direct handle to
                // Consumer.wakeup:()V — no INVOKEVIRTUAL appears in the outer method.
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, KafkaTypes.CONSUMER_OWNERS, "wakeup", "()V") != null) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean hasAnyKafkaListener(RuleContext ctx) {
        for (MethodNode mn : ctx.classNode().methods) {
            if (containsAnnotation(mn.visibleAnnotations, KafkaTypes.SPRING_KAFKA_LISTENER_ANNOTATION)
                    || containsAnnotation(mn.invisibleAnnotations, KafkaTypes.SPRING_KAFKA_LISTENER_ANNOTATION)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAnnotation(List<AnnotationNode> annots, String descriptor) {
        if (annots == null) return false;
        for (AnnotationNode a : annots) {
            if (descriptor.equals(a.desc)) return true;
        }
        return false;
    }
}
