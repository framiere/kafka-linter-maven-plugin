package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Fires for every {@code KafkaStreams.removeStreamThread()} call (the
 * no-argument overload, added in KIP-663 / Kafka 2.8). The bounded
 * overload {@code removeStreamThread(Duration)} imposes a wall-clock
 * deadline and returns {@code Optional.empty()} on timeout; the no-arg
 * variant waits with {@code Duration.ofMillis(Long.MAX_VALUE)} and parks
 * the caller forever if the selected thread is wedged in user code.
 *
 * <p>Descriptor matched: {@code ()Ljava/util/Optional;}. The bounded
 * overload {@code (Ljava/time/Duration;)Ljava/util/Optional;} is
 * intentionally not flagged.
 *
 * <h2>Method-reference capture is also flagged</h2>
 *
 * <p>The idiomatic dynamic-scale-down shape is to schedule the thread
 * removal onto an executor — for example
 * {@code scheduler.schedule(streams::removeStreamThread, 30, SECONDS)}
 * or {@code executor.submit(streams::removeStreamThread)}. Java resolves
 * {@code streams::removeStreamThread} captured into a no-arg SAM
 * ({@code Runnable.run()V}, {@code Callable.call()Ljava/lang/Object;},
 * {@code Supplier.get()Ljava/lang/Object;}) to the no-arg overload, so
 * the captured handle's descriptor is exactly the unsafe
 * {@code ()Ljava/util/Optional;} — the {@code Long.MAX_VALUE} wait
 * stands. (Captures into a 1-arg SAM like {@code Consumer<Duration>}
 * resolve to the bounded overload and are NOT a hazard.)
 *
 * <p>Without an indy walk the user-class bytecode contains zero
 * {@code INVOKE*} instructions targeting {@code removeStreamThread} —
 * the call happens inside the SAM adapter on whatever worker thread
 * later runs the captured target — and the canonical autoscaler-hangs
 * shape goes silently undetected.
 *
 * <p>Unlike {@code PRODUCER_SEND_NO_CALLBACK}, the SAM's return type is
 * NOT a discriminator here. The hazard is the API choice itself — the
 * no-arg overload's internal {@code Long.MAX_VALUE} timeout — which
 * holds whether or not the caller of the SAM observes the returned
 * {@code Optional<String>}. So we fire on every indy whose target
 * handle is the no-arg overload, regardless of {@code samMethodType}.
 */
public final class StreamsRemoveThreadNoTimeoutRule implements Rule {

    private static final String REMOVE_STREAM_THREAD = "removeStreamThread";
    private static final String REMOVE_NO_ARG_DESC = "()Ljava/util/Optional;";

    private final Severity severity;

    public StreamsRemoveThreadNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_REMOVE_THREAD_NO_TIMEOUT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && mi.getOpcode() == Opcodes.INVOKEVIRTUAL
                        && KafkaTypes.KAFKA_STREAMS.equals(mi.owner)
                        && REMOVE_STREAM_THREAD.equals(mi.name)
                        && REMOVE_NO_ARG_DESC.equals(mi.desc)) {
                    out.add(directCallViolation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(
                            indy, java.util.Set.of(KafkaTypes.KAFKA_STREAMS),
                            REMOVE_STREAM_THREAD, REMOVE_NO_ARG_DESC);
                    if (h == null) continue;
                    out.add(methodRefViolation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation directCallViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_REMOVE_THREAD_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KafkaStreams.removeStreamThread() (no-argument overload) is called "
                        + "here. The runtime picks the most-recently-added StreamThread, sets it "
                        + "to PENDING_SHUTDOWN, and waits for it to reach DEAD — internally "
                        + "bounded by Duration.ofMillis(Long.MAX_VALUE). If that thread is in "
                        + "the middle of a process() call that blocks (DB lookup, HTTP fetch, "
                        + "slow downstream producer), or if the producer is mid-commit on a "
                        + "transactional topology and the broker is slow to respond, the thread "
                        + "sits in PENDING_SHUTDOWN forever and the caller — typically an HTTP "
                        + "/scale-down handler, an autoscaler reconciliation loop, or a "
                        + "scheduled job — parks on the call. Dynamic-scaling deadlocks: the "
                        + "autoscaler's next decision never gets made; the HTTP worker thread "
                        + "is unavailable for tens of minutes. Use the bounded overload "
                        + "removeStreamThread(Duration) — feed the timeout from the surrounding "
                        + "request budget — and on Optional.empty() return log/retry rather "
                        + "than wait forever. Sibling [[streams-close-no-timeout]] catches the "
                        + "same Long.MAX_VALUE hazard on close().");
    }

    private Violation methodRefViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_REMOVE_THREAD_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "removeStreamThread() reference captured (e.g. "
                        + "executor.schedule(streams::removeStreamThread, 30, SECONDS)) onto a "
                        + "no-arg functional interface — the deferred call resolves to the "
                        + "no-argument overload and inherits its Long.MAX_VALUE wait. Whatever "
                        + "later thread invokes the SAM (a scheduled executor, a worker pool, a "
                        + "shutdown hook) parks on PENDING_SHUTDOWN forever if the targeted "
                        + "StreamThread is wedged in user code. Capture removeStreamThread("
                        + "Duration) instead — e.g. with a lambda that closes over the bounded "
                        + "timeout — and on Optional.empty() log/retry rather than wait "
                        + "forever.");
    }
}
