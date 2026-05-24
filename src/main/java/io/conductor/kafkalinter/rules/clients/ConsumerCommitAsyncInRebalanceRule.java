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
 * Fires when a class implementing {@code ConsumerRebalanceListener} calls
 * {@code commitAsync(...)} from inside {@code onPartitionsRevoked(Collection)}
 * or {@code onPartitionsLost(Collection)}. The rebalance state machine
 * proceeds synchronously once these callbacks return, so an async commit
 * posted inside the body does not actually persist before the partitions
 * are reassigned — the next consumer reads from the LAST PERSISTED offset
 * and re-processes the records the revoked consumer just processed.
 *
 * <p>{@code onPartitionsAssigned} is INTENTIONALLY excluded — committing
 * there is a different bug pattern.
 */
public final class ConsumerCommitAsyncInRebalanceRule implements Rule {

    private static final Set<String> REBALANCE_CALLBACK_NAMES = Set.of(
            "onPartitionsRevoked", "onPartitionsLost");
    private static final String REBALANCE_CALLBACK_DESC = "(Ljava/util/Collection;)V";
    private static final String COMMIT_ASYNC = "commitAsync";

    private final Severity severity;

    public ConsumerCommitAsyncInRebalanceRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_COMMITASYNC_IN_REBALANCE;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<String> interfaces = ctx.classNode().interfaces;
        if (interfaces == null || !interfaces.contains(KafkaTypes.CONSUMER_REBALANCE_LISTENER)) {
            return List.of();
        }
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            if (!REBALANCE_CALLBACK_NAMES.contains(mn.name)) continue;
            if (!REBALANCE_CALLBACK_DESC.equals(mn.desc)) continue;
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if (!COMMIT_ASYNC.equals(mi.name)) continue;

                out.add(new Violation(
                        RuleId.CONSUMER_COMMITASYNC_IN_REBALANCE, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "Consumer.commitAsync(...) inside ConsumerRebalanceListener." + mn.name
                                + " — commitAsync posts the OffsetCommitRequest to the consumer's outbound queue "
                                + "and returns IMMEDIATELY; the rebalance state machine proceeds as soon as this "
                                + "callback returns, so the commit does NOT complete before the partitions are "
                                + "reassigned. The new owner reads from the LAST PERSISTED offset and re-processes "
                                + "every record the revoked consumer just processed; downstream side-effects fire "
                                + "twice. Use commitSync(currentOffsets) instead — it blocks the callback until "
                                + "the broker has the offsets durably, so when the callback returns the offsets "
                                + "are committed and the rebalance is safe to proceed."));
            }
        }
        return out;
    }
}
