package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Fires when {@code producer.send(...)} is invoked inside a {@code Callback.onCompletion}
 * body. The callback runs on the producer's Sender thread; calling {@code send()} from it
 * blocks that thread on {@code max.block.ms} as soon as the accumulator is full,
 * self-deadlocking the producer.
 *
 * <p>Recognises two callback shapes:
 * <ul>
 *   <li>Named classes implementing {@code org.apache.kafka.clients.producer.Callback} —
 *       the {@code onCompletion(RecordMetadata, Exception)} method is the body.</li>
 *   <li>Lambda expressions: {@code INVOKEDYNAMIC} whose SAM-return type is
 *       {@code Callback}. The lambda body's synthetic method becomes the callback body.</li>
 * </ul>
 *
 * <p>Note: KAFKA-10852 added an explicit {@code KafkaException} for {@code flush()} and
 * {@code close()} from a Callback in 2.8+, but {@code send()} from a callback is NOT
 * explicitly blocked at runtime — it is exactly the anti-pattern that escapes runtime
 * detection and surfaces only under production load when the accumulator fills.
 */
public final class ProducerSendInCallbackRule implements Rule {

    private static final String ON_COMPLETION = "onCompletion";
    private static final String ON_COMPLETION_DESC =
            "(Lorg/apache/kafka/clients/producer/RecordMetadata;Ljava/lang/Exception;)V";
    private static final String CALLBACK_TYPE_DESC = "Lorg/apache/kafka/clients/producer/Callback;";
    private static final String SEND = "send";

    private final Severity severity;

    public ProducerSendInCallbackRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_SEND_IN_CALLBACK;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        ClassNode cn = ctx.classNode();
        Set<String> callbackMethodKeys = collectCallbackMethodKeys(cn);
        if (callbackMethodKeys.isEmpty()) return List.of();

        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : cn.methods) {
            if (!callbackMethodKeys.contains(mn.name + mn.desc)) continue;
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;
                if (!SEND.equals(mi.name)) continue;
                out.add(new Violation(
                        RuleId.PRODUCER_SEND_IN_CALLBACK, severity,
                        cn.name, mn.name, AsmUtil.lineOf(insn),
                        "producer.send() inside a Producer Callback — the callback runs on the "
                                + "producer's Sender thread, so send() blocks that same thread on "
                                + "max.block.ms as soon as the accumulator is full, self-deadlocking "
                                + "the producer. Hand the next send off to an executor (or a Streams "
                                + "topology / CompletableFuture chain) instead of doing it inline in "
                                + "the callback."));
            }
        }
        return out;
    }

    private static Set<String> collectCallbackMethodKeys(ClassNode cn) {
        Set<String> keys = new HashSet<>();
        if (cn.interfaces != null && cn.interfaces.contains(KafkaTypes.CALLBACK)) {
            for (MethodNode mn : cn.methods) {
                if (ON_COMPLETION.equals(mn.name) && ON_COMPLETION_DESC.equals(mn.desc)) {
                    keys.add(mn.name + mn.desc);
                }
            }
        }
        for (MethodNode mn : cn.methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof InvokeDynamicInsnNode indy)) continue;
                if (!isCallbackLambdaFactory(indy)) continue;
                Handle impl = findImplHandle(indy);
                if (impl == null) continue;
                if (!cn.name.equals(impl.getOwner())) continue;
                keys.add(impl.getName() + impl.getDesc());
            }
        }
        return keys;
    }

    private static boolean isCallbackLambdaFactory(InvokeDynamicInsnNode indy) {
        Handle bsm = indy.bsm;
        if (bsm == null) return false;
        if (!"java/lang/invoke/LambdaMetafactory".equals(bsm.getOwner())) return false;
        return indy.desc != null && indy.desc.endsWith(")" + CALLBACK_TYPE_DESC);
    }

    private static Handle findImplHandle(InvokeDynamicInsnNode indy) {
        if (indy.bsmArgs == null) return null;
        for (Object arg : indy.bsmArgs) {
            if (arg instanceof Handle h) return h;
        }
        return null;
    }
}
