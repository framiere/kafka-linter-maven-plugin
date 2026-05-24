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
 * Fires when {@code KafkaStreams.close(Duration.ZERO)} or
 * {@code KafkaStreams.close(Duration.ofXxx(0))} is called — short-circuits the
 * orderly shutdown of every StreamThread, leaving state stores unflushed, the
 * changelog producer's accumulator undrained and any open EOS transaction
 * fenced on the broker until {@code transaction.timeout.ms} expires.
 *
 * <p>The matched descriptor is {@code (Ljava/time/Duration;)Z} — that is the
 * bounded {@code KafkaStreams.close(Duration)} overload. The no-argument
 * {@code close()} ({@code ()V}) is covered by [[streams-close-no-timeout]];
 * the {@code close(CloseOptions)} overload carries its own
 * {@code Duration timeout} field and would need a separate rule.
 */
public final class StreamsCloseZeroDurationRule implements Rule {

    private static final String CLOSE_DURATION_DESC = "(Ljava/time/Duration;)Z";

    private final Severity severity;

    public StreamsCloseZeroDurationRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_CLOSE_ZERO_DURATION;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!isStreamsCloseDuration(insn)) continue;
                AbstractInsnNode arg = AsmUtil.prevSignificant(insn);
                String shape = AsmUtil.zeroDurationLiteralShape(arg);
                if (shape == null) continue;

                out.add(new Violation(
                        RuleId.STREAMS_CLOSE_ZERO_DURATION, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "KafkaStreams.close(" + shape + ") — short-circuits the orderly shutdown of every "
                                + "StreamThread. State-store caches are not flushed (RocksDB and the changelog "
                                + "topic both miss the in-cache writes; restart restores the store to a state "
                                + "the topology never emitted), the embedded producer's accumulator is not "
                                + "drained (silent data loss on every record still in flight), and for "
                                + "processing.guarantee=exactly_once_v2 the open transaction is abandoned "
                                + "without commitTransaction() or abortTransaction() — the transactional.id "
                                + "stays fenced on the broker until transaction.timeout.ms elapses (default "
                                + "60 s), and the next deploy fails with ProducerFencedException until the "
                                + "window expires. The embedded consumer never sends LeaveGroup, so the group "
                                + "coordinator waits session.timeout.ms before reassigning the dying "
                                + "instance's tasks — lag spikes on every deploy. Pass a Duration matched to "
                                + "your terminationGracePeriodSeconds minus a buffer, e.g. close(Duration"
                                + ".ofSeconds(20)) for a 30 s grace period, and check the boolean return — "
                                + "false means at least one thread did not stop in time and the application "
                                + "should log/alert/exit non-zero."));
            }
        }
        return out;
    }

    private static boolean isStreamsCloseDuration(AbstractInsnNode insn) {
        if (!(insn instanceof MethodInsnNode mi)) return false;
        if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL) return false;
        if (!KafkaTypes.KAFKA_STREAMS.equals(mi.owner)) return false;
        if (!"close".equals(mi.name)) return false;
        return CLOSE_DURATION_DESC.equals(mi.desc);
    }
}
