package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
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
 * Flags producer.send(record) when:
 *   1. The send overload taken is the single-arg one (no Callback), AND
 *   2. The returned Future is immediately discarded (next instruction is POP).
 *
 * That's the classic fire-and-forget pattern — send failures are silently
 * swallowed because nothing ever observes the Future or its callback.
 *
 * Acceptable forms (NOT flagged):
 *   - producer.send(record, (md, ex) -> { ... })   // has Callback
 *   - Future f = producer.send(record); ... f.get()  // Future is observed
 *   - return producer.send(record);                  // caller handles it
 */
public final class ProducerSendNoCallbackRule implements Rule {

    private final Severity severity;

    public ProducerSendNoCallbackRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_SEND_NO_CALLBACK;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;
                if (!mi.name.equals("send")) continue;
                if (AsmUtil.lastArgIsCallback(mi.desc)) continue; // callback overload — fine

                AbstractInsnNode next = AsmUtil.nextSignificant(insn);
                if (next instanceof InsnNode in && in.getOpcode() == Opcodes.POP) {
                    out.add(new Violation(
                            RuleId.PRODUCER_SEND_NO_CALLBACK,
                            severity,
                            ctx.classNode().name,
                            mn.name,
                            AsmUtil.lineOf(insn),
                            "send(record) without a Callback and the returned Future is discarded — errors will go unnoticed."));
                }
            }
        }
        return out;
    }
}
