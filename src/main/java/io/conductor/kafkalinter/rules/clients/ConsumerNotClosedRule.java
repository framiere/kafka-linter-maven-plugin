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
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Flags a {@code KafkaConsumer} that is constructed and used in a method but never closed
 * on the same local slot — the consumer never sends LeaveGroup, so the group coordinator
 * waits for {@code session.timeout.ms} of missed heartbeats (default 45 s) before
 * reassigning the dead member's partitions.
 *
 * <p>Conservative escape suppression: if the consumer reference is ever stored to a field
 * ({@code PUTFIELD}/{@code PUTSTATIC}) or returned ({@code ARETURN}) via an
 * {@code ALOAD N} that is the immediately-preceding significant instruction, the slot is
 * marked ESCAPED and the rule does not fire — another method may close it. This handles
 * the canonical "consumer is a field, closed in @PreDestroy" shape.
 *
 * <p>Try-with-resources is handled automatically: javac emits a synthetic {@code close()}
 * call in the generated finally region, which is detected like any other close.
 */
public final class ConsumerNotClosedRule implements Rule {

    private final Severity severity;

    public ConsumerNotClosedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_NOT_CLOSED;
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
        Map<Integer, AbstractInsnNode> consumerSlots = new HashMap<>();
        Set<Integer> usedSlots = new HashSet<>();
        Set<Integer> closedSlots = new HashSet<>();
        Set<Integer> escapedSlots = new HashSet<>();

        for (AbstractInsnNode insn : mn.instructions) {
            // (1) Track KafkaConsumer constructions: <init> + astore N
            if (insn instanceof MethodInsnNode ctor
                    && ctor.getOpcode() == Opcodes.INVOKESPECIAL
                    && "<init>".equals(ctor.name)
                    && KafkaTypes.KAFKA_CONSUMER.equals(ctor.owner)) {
                AbstractInsnNode next = AsmUtil.nextSignificant(ctor);
                if (next instanceof VarInsnNode v && v.getOpcode() == Opcodes.ASTORE) {
                    consumerSlots.put(v.var, ctor);
                }
                continue;
            }

            // (3) Detect escape: PUTFIELD/PUTSTATIC/ARETURN immediately preceded by ALOAD N
            int op = insn.getOpcode();
            if (op == Opcodes.PUTFIELD || op == Opcodes.PUTSTATIC || op == Opcodes.ARETURN) {
                AbstractInsnNode prev = AsmUtil.prevSignificant(insn);
                if (prev instanceof VarInsnNode v && v.getOpcode() == Opcodes.ALOAD
                        && consumerSlots.containsKey(v.var)) {
                    escapedSlots.add(v.var);
                }
                continue;
            }

            // (2) Consumer method calls — USED or CLOSED
            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, consumerSlots.keySet());
            if (slot == null) continue;

            if (mi.name.startsWith("close")) {
                closedSlots.add(slot);
            } else {
                usedSlots.add(slot);
            }
        }

        for (Map.Entry<Integer, AbstractInsnNode> e : consumerSlots.entrySet()) {
            int slot = e.getKey();
            if (!usedSlots.contains(slot)) continue;
            if (closedSlots.contains(slot)) continue;
            if (escapedSlots.contains(slot)) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_NOT_CLOSED, severity,
                    ctx.classNode().name, mn.name, AsmUtil.lineOf(e.getValue()),
                    "KafkaConsumer is constructed and used (subscribe/poll/assign/...) in this method "
                            + "but close() is never called on the same local slot. close() is the only "
                            + "path that sends LeaveGroup to the group coordinator; without it the "
                            + "coordinator only learns the member is dead after session.timeout.ms "
                            + "(default 45 s in 3.0+), stalling partitions for that window on every "
                            + "restart. Wrap the consumer in try-with-resources or close it in a "
                            + "finally / shutdown hook."));
        }
    }

}
