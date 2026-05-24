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
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Fires when {@code consumer.poll(...)} returning {@code ConsumerRecords} is
 * immediately followed by {@code POP} — the records are silently discarded
 * and the consumer's internal position advances past them, so the NEXT
 * {@code poll()} fetches the records AFTER the lost ones, and any
 * subsequent commit (auto-commit, {@code commitSync},
 * {@code commitAsync}) tells the broker the consumer processed through
 * the advanced position. Net effect: silent record loss with no
 * exception, no log line, and no metric — the consumer group's lag
 * keeps decreasing because the consumer DID move forward, even though
 * the application never saw the records.
 *
 * <h2>Detection</h2>
 *
 * <p>Bytecode signature:
 * <pre>
 *   ALOAD consumer
 *   ...args...
 *   INVOKEVIRTUAL/INVOKEINTERFACE poll(...)Lorg/apache/kafka/clients/consumer/ConsumerRecords;
 *   POP
 * </pre>
 *
 * <p>The rule matches any {@code poll} method on
 * {@code CONSUMER_OWNERS} whose descriptor ends in
 * {@code )Lorg/apache/kafka/clients/consumer/ConsumerRecords;} —
 * covering both {@code poll(Duration)} and the deprecated
 * {@code poll(long)} form.
 *
 * <p>{@code AsmUtil.nextSignificant(insn)} is used to skip
 * {@code LineNumberNode} / {@code LabelNode} / {@code FrameNode}
 * trivia between the {@code INVOKE} and the {@code POP} so the rule
 * is robust against the bytecode the compiler emits for source-line
 * breaks and frame metadata.
 *
 * <h2>What the rule deliberately does NOT match</h2>
 *
 * <ul>
 *   <li>{@code ConsumerRecords records = consumer.poll(timeout);} — the
 *       result is consumed by {@code ASTORE}, not {@code POP}. Even if
 *       the local is then never read, that is a different bug
 *       (unused local) handled by other tooling.</li>
 *   <li>{@code consumer.poll(timeout).iterator();} — the result is
 *       consumed by {@code INVOKEINTERFACE iterator}, not
 *       {@code POP}.</li>
 *   <li>{@code if (consumer.poll(timeout).isEmpty()) ...} — the result
 *       is consumed by {@code INVOKEINTERFACE isEmpty} then {@code IFEQ},
 *       not {@code POP}.</li>
 *   <li>{@code return consumer.poll(timeout);} — the result is
 *       consumed by {@code ARETURN}, not {@code POP}.</li>
 * </ul>
 */
public final class ConsumerPollResultIgnoredRule implements Rule {

    private static final String POLL = "poll";
    private static final String CONSUMER_RECORDS_RETURN_SUFFIX =
            ")Lorg/apache/kafka/clients/consumer/ConsumerRecords;";

    private final Severity severity;

    public ConsumerPollResultIgnoredRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_POLL_RESULT_IGNORED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if (!POLL.equals(mi.name)) continue;
                if (mi.desc == null || !mi.desc.endsWith(CONSUMER_RECORDS_RETURN_SUFFIX)) continue;
                AbstractInsnNode next = AsmUtil.nextSignificant(insn);
                if (!(next instanceof InsnNode in) || in.getOpcode() != Opcodes.POP) continue;
                out.add(new Violation(
                        RuleId.CONSUMER_POLL_RESULT_IGNORED, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "consumer.poll(...) called as a statement — the returned ConsumerRecords is "
                                + "immediately discarded (POP). The consumer's internal position advances "
                                + "past the fetched records, so the next poll() returns records AFTER the "
                                + "lost ones, and any subsequent commit (auto-commit, commitSync, "
                                + "commitAsync) tells the broker the consumer processed through the "
                                + "advanced position. Net effect: silent record loss with no exception, "
                                + "no log line, and no metric. If you mean to prime the consumer "
                                + "assignment, use `consumer.poll(Duration.ZERO)` followed by an "
                                + "explicit seek() BEFORE consuming. If you mean to drive heartbeats "
                                + "during a stall, tune max.poll.interval.ms / heartbeat.interval.ms or "
                                + "use consumer.pause(partitions) instead."));
            }
        }
        return out;
    }
}
