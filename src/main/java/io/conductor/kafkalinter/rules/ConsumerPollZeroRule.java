package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags {@code consumer.poll(...)} called with a literal-zero timeout. Both the
 * deprecated long-millis overload and the modern {@code Duration} overload are
 * covered, in every literal-zero shape javac (and other JVM bytecode
 * emitters) produce.
 *
 * <h2>Why this matters</h2>
 *
 * <p>{@code consumer.poll(timeout)} is the consumer's hot-path loop: it fetches
 * records, processes coordinator heartbeats, drains the fetcher network
 * buffers, and blocks UP TO {@code timeout} waiting for the broker to deliver
 * records. If {@code timeout} is zero, the call returns IMMEDIATELY regardless
 * of broker activity. The almost-universal calling shape
 * {@code while (running) { records = consumer.poll(t); process(records); }}
 * then becomes a tight busy-spin: a single thread pins one CPU core making
 * tens of thousands of poll() calls per second, while the broker pays the
 * fetch-request cost on every one of them and tail latency rises for every
 * OTHER consumer group on the same coordinator.
 *
 * <h2>Shapes detected</h2>
 *
 * <ul>
 *   <li>{@code poll(0L)} — long-millis overload preceded by the long-zero
 *       literal (bytecode {@code LCONST_0}).</li>
 *   <li>{@code poll(Duration.ZERO)} — Duration overload preceded by
 *       {@code GETSTATIC Duration.ZERO}.</li>
 *   <li>{@code poll(Duration.ofXxx(0))} for every {@code Duration} long-arg
 *       factory in {@link AsmUtil#DURATION_FACTORY_METHODS}, recognised via
 *       {@link AsmUtil#zeroDurationLiteralShape(AbstractInsnNode)}.</li>
 * </ul>
 *
 * <p>The Duration-zero shape recognition is shared with the close-Duration
 * sibling rules (Admin / Consumer / Producer close, Streams close, Streams
 * removeStreamThread) via {@link AsmUtil#zeroDurationLiteralShape} — every
 * rule that inspects a "zero {@code Duration}" argument flowing into a
 * Kafka client API goes through that one helper, so coverage cannot drift
 * apart. {@code ofMinutes(0)} / {@code ofHours(0)} / {@code ofDays(0)} are
 * not common in practice, but they are bit-exact equivalents of
 * {@code ofMillis(0)} and the literal {@code 0} can creep in via inlined
 * constants ({@code static final long IDLE_TIMEOUT = 0;}), generated code,
 * or refactors that erase a non-zero default — keeping detection complete
 * costs nothing and removes a future blind spot.
 *
 * <h2>What it does NOT fire on</h2>
 *
 * <p>Non-literal arguments — {@code poll(timeout)} where {@code timeout} is a
 * method parameter, field read, or computed value — are intentionally out of
 * scope. Dynamic-zero cases are a tiny minority of real occurrences and
 * proving them statically would require value-flow analysis; the cost in
 * false positives (and rule complexity) is not worth the few additional
 * fires. Literal-zero detection covers the overwhelming majority of
 * production occurrences of this anti-pattern.
 */
public final class ConsumerPollZeroRule implements Rule {

    private final Severity severity;

    public ConsumerPollZeroRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_POLL_ZERO;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if (!mi.name.equals("poll")) continue;

                Type[] args = Type.getArgumentTypes(mi.desc);
                if (args.length != 1) continue;

                AbstractInsnNode prev = AsmUtil.prevSignificant(insn);
                String shape = zeroTimeoutShape(prev, args[0]);
                if (shape == null) continue;

                out.add(new Violation(
                        RuleId.CONSUMER_POLL_ZERO,
                        severity,
                        ctx.classNode().name,
                        mn.name,
                        AsmUtil.lineOf(insn),
                        "consumer.poll(" + shape + ") — zero-timeout busy-spin. poll() returns "
                                + "immediately whether or not records are available, so the typical "
                                + "while-true polling loop becomes a tight CPU-bound spin: tens of "
                                + "thousands of fetch requests per second, one CPU core pinned, "
                                + "and tail latency rising for every other consumer group on the "
                                + "same coordinator. Pass a Duration matched to the consumer's "
                                + "back-pressure tolerance — typically 100ms to 1s."));
            }
        }
        return out;
    }

    private static String zeroTimeoutShape(AbstractInsnNode prev, Type argType) {
        if (prev == null) return null;
        if (argType.getSort() == Type.LONG) {
            return AsmUtil.isLongZeroLiteral(prev) ? "0L" : null;
        }
        if (!argType.getInternalName().equals(KafkaTypes.DURATION)) return null;
        return AsmUtil.zeroDurationLiteralShape(prev);
    }
}
