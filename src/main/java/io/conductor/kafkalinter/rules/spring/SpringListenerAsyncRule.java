package io.conductor.kafkalinter.rules.spring;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags methods annotated with both {@code @KafkaListener} and {@code @Async}.
 *
 * <p>The Kafka listener container commits offsets when the listener method returns.
 * {@code @Async} dispatches the body to a TaskExecutor and returns immediately, so the
 * container sees an instant "success" and commits before the real work runs. Any
 * exception in the async task is invisible to the listener container — no DLT, no
 * retry, silent message loss.
 */
public final class SpringListenerAsyncRule implements Rule {

    private final Severity severity;

    public SpringListenerAsyncRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SPRING_LISTENER_ASYNC_ANNOTATION;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            if (!hasAnnotation(mn.visibleAnnotations, KafkaTypes.SPRING_KAFKA_LISTENER_ANNOTATION)
                    && !hasAnnotation(mn.invisibleAnnotations, KafkaTypes.SPRING_KAFKA_LISTENER_ANNOTATION)) {
                continue;
            }
            if (!hasAnnotation(mn.visibleAnnotations, KafkaTypes.SPRING_ASYNC_ANNOTATION)
                    && !hasAnnotation(mn.invisibleAnnotations, KafkaTypes.SPRING_ASYNC_ANNOTATION)) {
                continue;
            }
            out.add(new Violation(
                    RuleId.SPRING_LISTENER_ASYNC_ANNOTATION, severity,
                    ctx.classNode().name, mn.name, 0,
                    "@KafkaListener method also annotated @Async — offsets commit before processing finishes, async failures are silently dropped."));
        }
        return out;
    }

    private static boolean hasAnnotation(List<AnnotationNode> annots, String descriptor) {
        if (annots == null) return false;
        for (AnnotationNode a : annots) {
            if (descriptor.equals(a.desc)) return true;
        }
        return false;
    }
}
