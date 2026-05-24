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
 * Fires when {@code producer.close(...)} is invoked inside a {@code Callback.onCompletion}
 * body — guaranteed deadlock because callbacks run on the producer's I/O / sender thread
 * which {@code close()} blocks on via {@code ioThread.join()}.
 *
 * <h2>Recognised callback shapes</h2>
 *
 * <ul>
 *   <li>Named classes implementing {@code org.apache.kafka.clients.producer.Callback} —
 *       the {@code onCompletion(RecordMetadata, Exception)} method is the body.</li>
 *   <li>Lambda expressions: {@code INVOKEDYNAMIC} whose SAM-return type is
 *       {@code Callback}. The lambda body's synthetic method becomes the callback body.</li>
 * </ul>
 *
 * <h2>Matched close() signatures</h2>
 *
 * <p>The rule matches ANY {@code close} method on {@code PRODUCER_OWNERS} regardless of
 * descriptor — no-arg ({@code ()V}), {@code Duration} ({@code (Ljava/time/Duration;)V}),
 * and the deprecated {@code (long, TimeUnit)} form
 * ({@code (JLjava/util/concurrent/TimeUnit;)V}). All three forms hit the same
 * {@code ioThread.join()} self-join path; from kafka-clients 0.11 onward the join is
 * downgraded to a zero-timeout forced shutdown with a logged warning instead of a hard
 * hang, but the lost-records consequence is the same.
 *
 * <h2>Mirrors {@link ProducerFlushInCallbackRule}</h2>
 *
 * <p>Same deadlock mechanism, same detection shape. The two rules are deliberately
 * separate: {@code flush()} blocks on accumulator drain, {@code close()} blocks on
 * sender-thread join. A reviewer reading a rule message should see exactly which call
 * site is the problem.
 */
public final class ProducerCloseInCallbackRule implements Rule {

    private static final String ON_COMPLETION = "onCompletion";
    private static final String ON_COMPLETION_DESC =
            "(Lorg/apache/kafka/clients/producer/RecordMetadata;Ljava/lang/Exception;)V";
    private static final String CALLBACK_TYPE_DESC = "Lorg/apache/kafka/clients/producer/Callback;";
    private static final String CLOSE = "close";

    private final Severity severity;

    public ProducerCloseInCallbackRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_CLOSE_IN_CALLBACK;
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
                if (!CLOSE.equals(mi.name)) continue;
                out.add(new Violation(
                        RuleId.PRODUCER_CLOSE_IN_CALLBACK, severity,
                        cn.name, mn.name, AsmUtil.lineOf(insn),
                        "producer.close() inside a Producer Callback — guaranteed deadlock (kafka-clients "
                                + "< 0.11) or silent zero-timeout forced shutdown that drops in-flight "
                                + "records (kafka-clients >= 0.11). Callbacks run on the producer's I/O "
                                + "thread; close() blocks on `ioThread.join()` waiting for that same thread "
                                + "to terminate. Move the close() call to the user thread (e.g. a shutdown "
                                + "hook or the main thread's finally block) and let the callback signal "
                                + "completion via a Future / latch / metric that the user thread reads."));
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
