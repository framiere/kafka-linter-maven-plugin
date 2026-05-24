package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Flags a consumer-lifecycle call (poll/commitSync/commitAsync/seek/subscribe/...) made on
 * a local slot AFTER {@code close()} was called on the same slot in the same method.
 *
 * <p>Direct mirror of {@link ProducerUsedAfterCloseRule}: same detection state machine,
 * same backward stack-effect simulation for receiver resolution, same method-reference
 * capture handling via {@code INVOKEDYNAMIC}. The only differences are the type
 * (KafkaConsumer instead of KafkaProducer) and the lifecycle-method set. {@code wakeup()}
 * is deliberately excluded — it is the only method documented as safe after close().
 */
public final class ConsumerUsedAfterCloseRule implements Rule {

    private static final Set<String> LIFECYCLE_METHODS = Set.of(
            "poll",
            "commitSync", "commitAsync",
            "subscribe", "unsubscribe", "assign",
            "assignment", "subscription",
            "seek", "seekToBeginning", "seekToEnd",
            "position", "committed",
            "beginningOffsets", "endOffsets", "offsetsForTimes",
            "pause", "resume", "paused",
            "partitionsFor", "listTopics",
            "enforceRebalance",
            "metrics",
            "groupMetadata",
            "currentLag");

    private final Severity severity;

    public ConsumerUsedAfterCloseRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_USED_AFTER_CLOSE;
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
        Set<Integer> consumerSlots = new HashSet<>();
        Set<Integer> closedSlots = new HashSet<>();

        for (AbstractInsnNode insn : mn.instructions) {
            // (1) Track KafkaConsumer constructions: new + <init> + astore N
            if (insn instanceof MethodInsnNode ctor
                    && ctor.getOpcode() == Opcodes.INVOKESPECIAL
                    && "<init>".equals(ctor.name)
                    && KafkaTypes.KAFKA_CONSUMER.equals(ctor.owner)) {
                AbstractInsnNode next = AsmUtil.nextSignificant(ctor);
                if (next instanceof VarInsnNode v && v.getOpcode() == Opcodes.ASTORE) {
                    consumerSlots.add(v.var);
                }
                continue;
            }

            // (2) Method-reference capture of a consumer lifecycle method:
            //     INVOKEDYNAMIC whose bsmArgs contain a REF_invokeVirtual
            //     KafkaConsumer.<lifecycle>(...) handle. Treat it as a use of the
            //     captured slot: if the slot is already closed, the deferred call
            //     will throw IllegalStateException when the captured functional
            //     interface is invoked on an executor / async thread.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                Handle h = AsmUtil.indyTargetHandle(indy, KafkaTypes.CONSUMER_OWNERS, null, null);
                if (h == null) continue;
                if (!LIFECYCLE_METHODS.contains(h.getName())) continue;
                Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, consumerSlots);
                for (Integer slot : captured) {
                    if (closedSlots.contains(slot)) {
                        out.add(new Violation(
                                RuleId.CONSUMER_USED_AFTER_CLOSE, severity,
                                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                                h.getName() + "(...) reference captured on a KafkaConsumer that was already "
                                        + "closed earlier in this method. close() is one-way — when the deferred "
                                        + "invocation runs (typically on an executor or async thread) it throws "
                                        + "IllegalStateException; in an async hand-off the exception is silently "
                                        + "swallowed and offset/poll state is corrupted. Move close() to the very "
                                        + "last call on the consumer."));
                        break;
                    }
                }
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;

            if (mi.name.startsWith("close")) {
                Integer slot = AsmUtil.resolveReceiverSlot(mi, consumerSlots);
                if (slot != null) closedSlots.add(slot);
                continue;
            }

            if (!LIFECYCLE_METHODS.contains(mi.name)) continue;
            Integer slot = AsmUtil.resolveReceiverSlot(mi, consumerSlots);
            if (slot == null || !closedSlots.contains(slot)) continue;

            out.add(new Violation(
                    RuleId.CONSUMER_USED_AFTER_CLOSE, severity,
                    ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                    mi.name + "(...) called on a KafkaConsumer that was already closed earlier "
                            + "in this method. close() is one-way — any subsequent call throws "
                            + "IllegalStateException; in an async hand-off the exception is silently "
                            + "swallowed and offset/poll state is corrupted. Move close() to the very "
                            + "last call on the consumer."));
        }
    }
}
