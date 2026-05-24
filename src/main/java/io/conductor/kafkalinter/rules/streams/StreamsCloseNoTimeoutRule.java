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
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every {@code KafkaStreams.close()} call (the no-argument
 * overload). The bounded overloads {@code close(Duration)} and
 * {@code close(CloseOptions)} (which carries a timeout) impose a wall-clock
 * deadline; the no-arg variant delegates to
 * {@code close(Duration.ofMillis(Long.MAX_VALUE))} and parks the caller
 * forever if any StreamThread is wedged in user code.
 *
 * <p>The descriptor we match is {@code ()V} — the no-arg overload. Calls
 * to {@code close(Duration)} ({@code (Ljava/time/Duration;)Z}) and
 * {@code close(CloseOptions)}
 * ({@code (Lorg/apache/kafka/streams/KafkaStreams$CloseOptions;)Z}) have
 * different descriptors and are intentionally not flagged.
 *
 * <p>Method references like {@code streams::close} ARE flagged. javac
 * compiles {@code new Thread(streams::close)} to an {@code INVOKEDYNAMIC}
 * whose bootstrap-method arguments include a direct
 * {@code REF_invokeVirtual KafkaStreams.close:()V} handle — no
 * {@code lambda$N} body, no {@code INVOKEVIRTUAL} in the outer method. The
 * deferred call has the same effect as a direct no-arg close.
 */
public final class StreamsCloseNoTimeoutRule implements Rule {

    private static final String CLOSE = "close";
    private static final String CLOSE_NO_ARG_DESC = "()V";

    private final Severity severity;

    public StreamsCloseNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_CLOSE_NO_TIMEOUT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && mi.getOpcode() == Opcodes.INVOKEVIRTUAL
                        && KafkaTypes.KAFKA_STREAMS.equals(mi.owner)
                        && CLOSE.equals(mi.name)
                        && CLOSE_NO_ARG_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, Set.of(KafkaTypes.KAFKA_STREAMS), CLOSE, CLOSE_NO_ARG_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_CLOSE_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KafkaStreams.close() (no-argument overload) is reached here — either as a "
                        + "direct call or as a method-reference capture (e.g. `streams::close`) whose "
                        + "deferred invocation has the same effect. "
                        + "Internally this delegates to close(Duration.ofMillis(Long.MAX_VALUE)) "
                        + "— the caller blocks until every StreamThread reaches the DEAD state. "
                        + "If any thread is wedged in user code (a slow Processor, a blocked "
                        + "HTTP call, a punctuator doing heavy DB work, a producer flush waiting "
                        + "on an unavailable broker), the latch never decrements and close() "
                        + "never returns. In Kubernetes this means the pod's "
                        + "terminationGracePeriodSeconds (default 30 s) expires, the kubelet "
                        + "sends SIGKILL, in-flight EOS transactions are torn mid-commit, and "
                        + "the rolling deployment leaves stuck partitions waiting on "
                        + "transactional-fencing timeouts. Use the bounded overload "
                        + "close(Duration) — pick a timeout slightly below your pod's "
                        + "terminationGracePeriodSeconds (e.g. Duration.ofSeconds(20) for the "
                        + "default 30 s grace) and check the boolean return: false means "
                        + "threads did not stop in time and the application should log/alert "
                        + "and exit non-zero rather than pretend the shutdown succeeded.");
    }
}
