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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Flags a producer-lifecycle call (send/flush/beginTransaction/...) made on a local
 * slot AFTER {@code close()} was called on the same slot in the same method.
 *
 * <p>Receiver resolution uses {@link AsmUtil#resolveReceiverSlot(MethodInsnNode, Set)},
 * which performs backward stack-effect simulation to robustly identify the receiver
 * even when arg expressions contain nested method calls (e.g.,
 * {@code send(new ProducerRecord(...))} or {@code close(Duration.ofSeconds(5))}).
 * Returns {@code null} only when an instruction with unknown stack effects is
 * encountered — acceptable false-negative.
 */
public final class ProducerUsedAfterCloseRule implements Rule {

    private static final Set<String> LIFECYCLE_METHODS = Set.of(
            "send", "flush",
            "beginTransaction", "commitTransaction", "abortTransaction",
            "initTransactions", "sendOffsetsToTransaction",
            "partitionsFor", "metrics");

    private final Severity severity;

    public ProducerUsedAfterCloseRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_USED_AFTER_CLOSE;
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
        Set<Integer> producerSlots = new HashSet<>();
        Set<Integer> closedSlots = new HashSet<>();

        for (AbstractInsnNode insn : mn.instructions) {
            // (1) Track KafkaProducer constructions: new + <init> + astore N
            if (insn instanceof MethodInsnNode ctor
                    && ctor.getOpcode() == Opcodes.INVOKESPECIAL
                    && "<init>".equals(ctor.name)
                    && KafkaTypes.KAFKA_PRODUCER.equals(ctor.owner)) {
                AbstractInsnNode next = AsmUtil.nextSignificant(ctor);
                if (next instanceof VarInsnNode v && v.getOpcode() == Opcodes.ASTORE) {
                    producerSlots.add(v.var);
                }
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;

            if (mi.name.startsWith("close")) {
                Integer slot = AsmUtil.resolveReceiverSlot(mi, producerSlots);
                if (slot != null) closedSlots.add(slot);
                continue;
            }

            if (!LIFECYCLE_METHODS.contains(mi.name)) continue;
            Integer slot = AsmUtil.resolveReceiverSlot(mi, producerSlots);
            if (slot == null || !closedSlots.contains(slot)) continue;

            out.add(new Violation(
                    RuleId.PRODUCER_USED_AFTER_CLOSE, severity,
                    ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                    mi.name + "(...) called on a KafkaProducer that was already closed earlier "
                            + "in this method. close() is one-way — any subsequent call throws "
                            + "IllegalStateException; in an async callback the exception is silently "
                            + "swallowed and the record is lost. Move close() to the very last call "
                            + "on the producer."));
        }
    }

}
