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
 * Flags a class that calls both {@code KafkaConsumer.assign(...)} and
 * {@code KafkaConsumer.subscribe(...)} — the two are mutually exclusive consumer modes
 * and the Java client throws {@code IllegalStateException} on the second call.
 *
 * <p>Detection is per-class rather than per-consumer-instance: we don't track which
 * consumer the calls land on. This produces false positives only in the unusual case
 * of a class operating two distinct consumers in deliberately different modes — rare
 * enough that one MEDIUM-confidence flag is the right tradeoff.
 */
public final class ConsumerAssignAndSubscribeRule implements Rule {

    private final Severity severity;

    public ConsumerAssignAndSubscribeRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_ASSIGN_AND_SUBSCRIBE;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        AbstractInsnNode assignInsn = null;
        String assignMethod = null;
        AbstractInsnNode subscribeInsn = null;
        String subscribeMethod = null;

        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if ("assign".equals(mi.name) && assignInsn == null) {
                    assignInsn = insn;
                    assignMethod = mn.name;
                } else if ("subscribe".equals(mi.name) && subscribeInsn == null) {
                    subscribeInsn = insn;
                    subscribeMethod = mn.name;
                }
            }
        }

        if (assignInsn == null || subscribeInsn == null) return List.of();

        List<Violation> out = new ArrayList<>(1);
        out.add(new Violation(
                RuleId.CONSUMER_ASSIGN_AND_SUBSCRIBE, severity,
                ctx.classNode().name, subscribeMethod, AsmUtil.lineOf(subscribeInsn),
                "Class calls both .assign() (at " + assignMethod + ":" + AsmUtil.lineOf(assignInsn)
                        + ") and .subscribe() — mutually exclusive consumer modes."));
        return out;
    }
}
