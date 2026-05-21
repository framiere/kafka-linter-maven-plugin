package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
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
 * <p>Receiver resolution uses {@code Type.getArgumentTypes(desc).length} to walk
 * back {@code (args + 1)} significant instructions; the receiver is identified
 * only when each walk-back lands on a single-instruction push and the final
 * step is an {@code ALOAD} of a tracked producer slot. Complex arg shapes (e.g.,
 * {@code send(new ProducerRecord(...))}) cause the walk-back to land on a
 * non-ALOAD insn — the rule bails on those (acceptable false-negative).
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
                Integer slot = receiverSlot(mi, producerSlots);
                if (slot != null) closedSlots.add(slot);
                continue;
            }

            if (!LIFECYCLE_METHODS.contains(mi.name)) continue;
            Integer slot = receiverSlot(mi, producerSlots);
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

    /**
     * Resolve the local slot that holds the receiver of {@code mi}. Walks back
     * {@code (argCount + 1)} significant instructions and checks the final step
     * is an {@code ALOAD} into a tracked producer slot. Returns {@code null}
     * when the walk-back cannot resolve a simple receiver — the rule then
     * accepts the false-negative.
     */
    private static Integer receiverSlot(MethodInsnNode mi, Set<Integer> producerSlots) {
        int argCount = Type.getArgumentTypes(mi.desc).length;
        AbstractInsnNode cursor = mi;
        for (int i = 0; i <= argCount; i++) {
            cursor = AsmUtil.prevSignificant(cursor);
            if (cursor == null) return null;
        }
        if (cursor instanceof VarInsnNode v && v.getOpcode() == Opcodes.ALOAD
                && producerSlots.contains(v.var)) {
            return v.var;
        }
        return null;
    }
}
