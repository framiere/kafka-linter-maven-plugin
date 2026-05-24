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
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Fires for every reach of {@code Consumer.poll(long)} — the deprecated
 * primitive-long overload — whether as a direct {@code INVOKEINTERFACE} /
 * {@code INVOKEVIRTUAL} or as an {@code INVOKEDYNAMIC} method-reference
 * capture (e.g. {@code consumer::poll} stored into a
 * {@code Function<Long, ConsumerRecords>}).
 *
 * <h2>Why we discriminate by descriptor, not by name</h2>
 *
 * <p>The {@code poll} method on {@code Consumer} / {@code KafkaConsumer} has
 * two overloads:
 * <ul>
 *   <li>{@code poll(long timeout)} — descriptor prefix {@code (J)}, deprecated
 *       since Kafka 2.0 (KIP-266), still present in 3.x but slated for removal.</li>
 *   <li>{@code poll(Duration timeout)} — descriptor prefix
 *       {@code (Ljava/time/Duration;)}, the supported replacement.</li>
 * </ul>
 * A name-only match would flag every poll site indiscriminately — including
 * the recommended {@code poll(Duration)} usage that the catalog wants left
 * alone. So the matcher pins the descriptor: a direct call qualifies only if
 * {@code mi.desc.startsWith("(J)")}; an indy capture qualifies only if the
 * resolved handle's descriptor starts with {@code (J)}.
 *
 * <h2>The semantic gap between the two overloads</h2>
 *
 * <p>{@code poll(long)} carries a documented but easily-missed
 * &laquo;join the group first, then time the wait&raquo; behaviour: if the
 * consumer is not yet a member of its consumer group (e.g. cold start,
 * coordinator move, recent rebalance), {@code poll(long)} blocks
 * INDEFINITELY waiting for the initial assignment regardless of the supplied
 * timeout. The timeout argument applies only to the subsequent fetch wait.
 * {@code poll(Duration)} fixed this: the supplied duration is a TOTAL
 * deadline covering both assignment and fetch — if the consumer cannot
 * join the group within the budget, {@code poll} returns an empty
 * {@code ConsumerRecords} and lets the caller decide what to do.
 *
 * <h2>Operational impact</h2>
 *
 * <p>The hazard surfaces during operations: a broker restart that triggers
 * a controller / coordinator election can leave consumers stuck inside
 * {@code poll(long)} for the entire election window because the timeout
 * does not bound the join phase. The thread is in {@code TIMED_WAITING}
 * but the timeout it&rsquo;s timed-waiting against has not even started ticking
 * yet. Liveness probes that only check "is the consumer thread alive?"
 * report green; the consumer is alive but making no progress. Switching
 * to {@code poll(Duration)} converts this from a silent stall into a
 * visible loop of empty record batches that the caller can log or alert on.
 *
 * <h2>Method-reference capture</h2>
 *
 * <p>{@code consumer::poll} compiles to an {@code INVOKEDYNAMIC} whose
 * bootstrap-method args include a {@code REF_invokeInterface} (or
 * {@code REF_invokeVirtual} for {@code KafkaConsumer}) handle to {@code poll}.
 * The Java compiler resolves the overload at capture time based on the
 * functional-interface SAM signature — assigning {@code consumer::poll}
 * to {@code LongFunction<ConsumerRecords<K,V>>} captures the {@code (J)}
 * overload; assigning to {@code Function<Duration, ConsumerRecords<K,V>>}
 * captures the {@code (Ljava/time/Duration;)} overload. The rule's
 * {@code INVOKEDYNAMIC} walk inspects the resolved handle&rsquo;s descriptor
 * and only flags the long-prefix one.
 */
public final class ConsumerPollLongDeprecatedRule implements Rule {

    private static final String POLL = "poll";
    private static final String LONG_DESC_PREFIX = "(J)";

    private final Severity severity;

    public ConsumerPollLongDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_POLL_LONG_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)
                        && POLL.equals(mi.name)
                        && mi.desc != null
                        && mi.desc.startsWith(LONG_DESC_PREFIX)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, KafkaTypes.CONSUMER_OWNERS, POLL, null);
                    if (h != null && h.getDesc() != null && h.getDesc().startsWith(LONG_DESC_PREFIX)) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.CONSUMER_POLL_LONG_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.poll(long) is reached here — either as a direct call or as a "
                        + "method-reference capture (e.g. `consumer::poll` assigned to a "
                        + "LongFunction) whose deferred invocation has the same deprecated "
                        + "semantics. This overload is deprecated since Kafka 2.0 (KIP-266) "
                        + "and slated for removal. The long overload has a subtle hazard: if "
                        + "the consumer is not yet a member of its consumer group (cold start, "
                        + "coordinator move, recent rebalance), poll(long) blocks INDEFINITELY "
                        + "waiting for the initial group assignment regardless of the supplied "
                        + "timeout — the timeout argument only bounds the subsequent fetch wait. "
                        + "Replace with poll(Duration), where the supplied Duration is a TOTAL "
                        + "deadline covering both group-join and fetch: if the consumer cannot "
                        + "join within the budget, poll returns an empty ConsumerRecords and "
                        + "the caller learns about the coordinator outage instead of silently "
                        + "stalling. Operationally this matters because liveness probes that "
                        + "only check 'consumer thread alive?' report green while a consumer "
                        + "is parked inside poll(long) for the whole controller-election "
                        + "window — the thread is in TIMED_WAITING against a clock that hasn't "
                        + "started. Equivalent safe call: consumer.poll(Duration.ofMillis(500)).");
    }
}
