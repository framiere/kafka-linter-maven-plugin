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
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL) continue;
                if (!KafkaTypes.KAFKA_STREAMS.equals(mi.owner)) continue;
                if (!REMOVE_STREAM_THREAD.equals(mi.name)) continue;
                if (!REMOVE_NO_ARG_DESC.equals(mi.desc)) continue;
                out.add(new Violation(
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
                                + "same Long.MAX_VALUE hazard on close()."));
            }
        }
        return out;
    }
}
