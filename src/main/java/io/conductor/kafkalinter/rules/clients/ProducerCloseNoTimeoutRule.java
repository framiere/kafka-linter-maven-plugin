package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Fires for every {@code Producer.close()} call with the no-argument
 * descriptor. The bounded overload {@code close(Duration)} enforces a
 * wall-clock deadline; the no-arg variant delegates to
 * {@code close(Duration.ofMillis(Long.MAX_VALUE))} and waits indefinitely
 * for every record still in the accumulator to either succeed or burn its
 * full {@code delivery.timeout.ms} budget.
 *
 * <p>The rule matches calls whose method name is {@code close} and whose
 * descriptor is {@code ()V} (no arguments, void return), on either the
 * {@code Producer} interface or the {@code KafkaProducer} concrete class.
 * The {@code close(Duration)} overload (descriptor
 * {@code (Ljava/time/Duration;)V}) is intentionally not flagged.
 */
public final class ProducerCloseNoTimeoutRule implements Rule {

    private static final String CLOSE = "close";
    private static final String CLOSE_NO_ARG_DESC = "()V";

    private final Severity severity;

    public ProducerCloseNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_CLOSE_NO_TIMEOUT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;
                if (!CLOSE.equals(mi.name)) continue;
                if (!CLOSE_NO_ARG_DESC.equals(mi.desc)) continue;
                out.add(new Violation(
                        RuleId.PRODUCER_CLOSE_NO_TIMEOUT, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "Producer.close() (no-argument overload) is called here. Internally "
                                + "this delegates to close(Duration.ofMillis(Long.MAX_VALUE)) — the "
                                + "caller stops accepting new sends, then parks until the Sender thread "
                                + "has drained the accumulator. Every record still in flight has up to "
                                + "delivery.timeout.ms (default 120 s) to land before the producer "
                                + "considers it failed; with retries enabled this stretches further. On "
                                + "a partition with acks=all and unreachable brokers, a producer with "
                                + "100 unflushed records can block close() for 100 × delivery.timeout.ms "
                                + "in the worst case. In Kubernetes this is the single most common "
                                + "cause of 'pod stuck terminating': the shutdown hook calls "
                                + "producer.close(), the calling thread parks for minutes during a "
                                + "broker outage, terminationGracePeriodSeconds (default 30 s) expires, "
                                + "SIGKILL fires, and the JVM dies mid-flush — leaving any in-flight "
                                + "transactional commit hanging on transaction.timeout.ms broker-side. "
                                + "Use the bounded overload close(Duration). Pick a timeout matching "
                                + "your pod's terminationGracePeriodSeconds minus a buffer "
                                + "(e.g. Duration.ofSeconds(20) for a 30 s grace) and treat 'close did "
                                + "not complete in time' as a metric/alert signal rather than a hidden "
                                + "data-loss event."));
            }
        }
        return out;
    }
}
