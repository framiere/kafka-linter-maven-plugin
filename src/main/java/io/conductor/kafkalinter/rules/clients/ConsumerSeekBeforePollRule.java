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
 * Flags {@code Consumer.seek(...)} that appears, in linear bytecode order, after
 * {@code Consumer.subscribe(...)} and before any {@code Consumer.poll(...)} in the same
 * method. Until poll() completes the group join, assignment() is empty and seek()
 * throws {@code IllegalStateException}. Manual {@code Consumer.assign(...)} resets
 * the state (assign synchronously populates assignment()).
 */
public final class ConsumerSeekBeforePollRule implements Rule {

    private enum State { INITIAL, SUBSCRIBED, POLLED, MANUAL_ASSIGN }

    private final Severity severity;

    public ConsumerSeekBeforePollRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_SEEK_BEFORE_POLL;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            scanMethod(out, ctx, mn);
        }
        return out;
    }

    private void scanMethod(List<Violation> out, RuleContext ctx, MethodNode mn) {
        State state = State.INITIAL;
        for (AbstractInsnNode insn : mn.instructions) {
            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;

            switch (mi.name) {
                case "assign" -> state = State.MANUAL_ASSIGN;
                case "subscribe" -> { if (state == State.INITIAL) state = State.SUBSCRIBED; }
                case "poll" -> { if (state == State.SUBSCRIBED) state = State.POLLED; }
                case "seek" -> {
                    if (state == State.SUBSCRIBED) {
                        out.add(new Violation(
                                RuleId.CONSUMER_SEEK_BEFORE_POLL, severity,
                                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                                "Consumer.seek(...) follows subscribe() with no intervening poll() — assignment() is empty until the first poll, so seek throws IllegalStateException. Move seek into ConsumerRebalanceListener.onPartitionsAssigned, or use Consumer.assign(...) for manual partition assignment."));
                    }
                }
                default -> { /* other consumer calls do not affect state */ }
            }
        }
    }
}
