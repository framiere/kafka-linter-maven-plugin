package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags {@code new KafkaProducer(...)} inside a method annotated with an HTTP/REST handler
 * annotation (Spring MVC, JAX-RS / Quarkus REST). The handler is called per HTTP request,
 * so a producer constructed inside it pays the full network-thread + Sender-thread +
 * accumulator + metadata-fetch + TLS-handshake cost on every call.
 *
 * <p>{@code KafkaProducer} is documented as thread-safe and designed to be shared across
 * threads as a long-lived singleton — hoist the construction into a constructor /
 * {@code @PostConstruct} / {@code @Bean} method and inject the producer as a field.
 */
public final class ProducerPerRecordAllocationRule implements Rule {

    private final Severity severity;

    public ProducerPerRecordAllocationRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PER_RECORD_ALLOCATION;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            if (!isHttpHandler(mn)) continue;
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (mi.getOpcode() != Opcodes.INVOKESPECIAL) continue;
                if (!"<init>".equals(mi.name)) continue;
                if (!KafkaTypes.KAFKA_PRODUCER.equals(mi.owner)) continue;
                out.add(new Violation(
                        RuleId.PRODUCER_PER_RECORD_ALLOCATION, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "new KafkaProducer(...) inside an HTTP/REST handler method — the "
                                + "producer is built per request, paying full network-thread, "
                                + "Sender-thread, accumulator (buffer.memory = 32 MiB default), "
                                + "metadata-fetch (max.block.ms = 60 s) and TLS-handshake cost on "
                                + "every call. KafkaProducer is thread-safe and designed to be "
                                + "shared across threads; construct it once in a constructor / "
                                + "@PostConstruct / @Bean and inject it as a field."));
            }
        }
        return out;
    }

    private static boolean isHttpHandler(MethodNode mn) {
        return hasAnyAnnotation(mn.visibleAnnotations) || hasAnyAnnotation(mn.invisibleAnnotations);
    }

    private static boolean hasAnyAnnotation(List<AnnotationNode> annots) {
        if (annots == null) return false;
        for (AnnotationNode a : annots) {
            if (KafkaTypes.HTTP_HANDLER_ANNOTATIONS.contains(a.desc)) return true;
        }
        return false;
    }
}
