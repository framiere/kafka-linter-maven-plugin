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
import java.util.Set;

/**
 * Flags {@code consumer.poll(...)} calls inside a {@code ConsumerRebalanceListener}
 * callback. The callback runs on the poll thread mid-rebalance; calling poll
 * recursively re-enters the rebalance state machine and throws
 * {@code IllegalStateException} in modern Kafka. The other consumer methods
 * (commitSync, seek, committed, position) are allowed — only poll is illegal.
 */
public final class PollInRebalanceCallbackRule implements Rule {

    private static final Set<String> CALLBACK_NAMES = Set.of(
            "onPartitionsRevoked", "onPartitionsAssigned", "onPartitionsLost");
    private static final String CALLBACK_DESC = "(Ljava/util/Collection;)V";

    private final Severity severity;

    public PollInRebalanceCallbackRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.POLL_IN_REBALANCE_CALLBACK;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        if (!implementsRebalanceListener(ctx)) return List.of();
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            if (!CALLBACK_NAMES.contains(mn.name)) continue;
            if (!CALLBACK_DESC.equals(mn.desc)) continue;
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if (!"poll".equals(mi.name)) continue;

                out.add(new Violation(
                        RuleId.POLL_IN_REBALANCE_CALLBACK, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "Consumer.poll() inside ConsumerRebalanceListener." + mn.name + " — the callback runs on the poll thread mid-rebalance; recursive poll throws IllegalStateException."));
            }
        }
        return out;
    }

    private static boolean implementsRebalanceListener(RuleContext ctx) {
        List<String> interfaces = ctx.classNode().interfaces;
        return interfaces != null && interfaces.contains(KafkaTypes.CONSUMER_REBALANCE_LISTENER);
    }
}
