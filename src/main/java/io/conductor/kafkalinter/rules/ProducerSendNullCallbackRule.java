package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
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
 * Fires when {@code producer.send(record, null)} is called with a literal
 * {@code null} Callback — the two-argument overload was chosen but no error
 * handler was provided. Distinct from {@link ProducerSendNoCallbackRule}, which
 * targets the one-argument {@code send(record)} form with a discarded Future.
 */
public final class ProducerSendNullCallbackRule implements Rule {

    private final Severity severity;

    public ProducerSendNullCallbackRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_SEND_NULL_CALLBACK;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL && mi.getOpcode() != Opcodes.INVOKEINTERFACE) continue;
                if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;
                if (!"send".equals(mi.name)) continue;
                if (!AsmUtil.lastArgIsCallback(mi.desc)) continue;

                AbstractInsnNode prev = AsmUtil.prevSignificant(insn);
                if (prev == null || prev.getOpcode() != Opcodes.ACONST_NULL) continue;

                out.add(new Violation(
                        RuleId.PRODUCER_SEND_NULL_CALLBACK, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "producer.send(record, null) — explicit two-arg overload with a literal null Callback. "
                                + "Identical runtime semantics to the no-callback overload (errors swallowed) but "
                                + "looks deliberate in source. Pass a real Callback that logs/metrics the "
                                + "(meta, exception) pair, or drop the second argument so fire-and-forget is "
                                + "syntactically explicit."));
            }
        }
        return out;
    }
}
