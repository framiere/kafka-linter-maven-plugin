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
 * Flags a {@code KafkaProducer} that is constructed and used in a method but never
 * closed on the same local slot — on JVM exit the Sender thread is killed mid-batch,
 * accumulator records are silently dropped, and for a transactional producer the
 * in-flight transaction stays open until {@code transaction.timeout.ms}.
 *
 * <p>Escape suppression: if the producer reference is stored to a field
 * ({@code PUTFIELD}/{@code PUTSTATIC}) or returned ({@code ARETURN}) via an
 * {@code ALOAD N} that is the immediately-preceding significant instruction, the slot
 * is marked ESCAPED and the rule does not fire — another method may close it.
 *
 * <p>Try-with-resources is handled automatically: javac emits a synthetic
 * {@code close()} call in the generated finally region, which is detected like any
 * other close.
 */
public final class ProducerNotClosedRule implements Rule {

    private final Severity severity;

    public ProducerNotClosedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_NOT_CLOSED;
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
        Map<Integer, AbstractInsnNode> producerSlots = new HashMap<>();
        Set<Integer> usedSlots = new HashSet<>();
        Set<Integer> closedSlots = new HashSet<>();
        Set<Integer> escapedSlots = new HashSet<>();

        for (AbstractInsnNode insn : mn.instructions) {
            // (1) Track KafkaProducer constructions: <init> + astore N
            if (insn instanceof MethodInsnNode ctor
                    && ctor.getOpcode() == Opcodes.INVOKESPECIAL
                    && "<init>".equals(ctor.name)
                    && KafkaTypes.KAFKA_PRODUCER.equals(ctor.owner)) {
                AbstractInsnNode next = AsmUtil.nextSignificant(ctor);
                if (next instanceof VarInsnNode v && v.getOpcode() == Opcodes.ASTORE) {
                    producerSlots.put(v.var, ctor);
                }
                continue;
            }

            // (3) Detect escape: PUTFIELD/PUTSTATIC/ARETURN immediately preceded by ALOAD N
            int op = insn.getOpcode();
            if (op == Opcodes.PUTFIELD || op == Opcodes.PUTSTATIC || op == Opcodes.ARETURN) {
                AbstractInsnNode prev = AsmUtil.prevSignificant(insn);
                if (prev instanceof VarInsnNode v && v.getOpcode() == Opcodes.ALOAD
                        && producerSlots.containsKey(v.var)) {
                    escapedSlots.add(v.var);
                }
                continue;
            }

            // (2) Producer method calls — USED or CLOSED
            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, producerSlots.keySet());
            if (slot == null) continue;

            if (mi.name.startsWith("close")) {
                closedSlots.add(slot);
            } else {
                usedSlots.add(slot);
            }
        }

        for (Map.Entry<Integer, AbstractInsnNode> e : producerSlots.entrySet()) {
            int slot = e.getKey();
            if (!usedSlots.contains(slot)) continue;
            if (closedSlots.contains(slot)) continue;
            if (escapedSlots.contains(slot)) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_NOT_CLOSED, severity,
                    ctx.classNode().name, mn.name, AsmUtil.lineOf(e.getValue()),
                    "KafkaProducer is constructed and used (send/flush/...) in this method "
                            + "but close() is never called on the same local slot. close() is the only "
                            + "path that flushes the accumulator: without it, JVM shutdown kills the "
                            + "Sender thread mid-batch and every record still sitting in the buffer "
                            + "(up to buffer.memory = 32 MiB by default) is silently dropped. For a "
                            + "transactional producer the in-flight transaction stays open until "
                            + "transaction.timeout.ms (default 60 s), stalling downstream "
                            + "read_committed consumers. Wrap the producer in try-with-resources or "
                            + "close it from a shutdown hook."));
        }
    }
}
