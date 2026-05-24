package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
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
 * Flags a {@code KafkaStreams} instance that is constructed and used (typically
 * via {@code start()}) in a method but never {@code close()}-d on the same local
 * slot. On JVM exit without an orderly {@code streams.close()}: the StreamThreads
 * are killed mid-batch, the consumer-group membership is never released, the broker
 * keeps the session alive until {@code session.timeout.ms} (default 45 s in Streams
 * 3.x), and downstream partition reassignment is delayed by that amount.
 *
 * <p>Escape suppression mirrors {@link io.conductor.kafkalinter.rules.clients.ProducerNotClosedRule}:
 * a slot stored to a field ({@code PUTFIELD}/{@code PUTSTATIC}) or returned
 * ({@code ARETURN}) is marked ESCAPED and the rule does not fire — another method
 * may close it.
 *
 * <p>Try-with-resources is handled automatically: {@code KafkaStreams} implements
 * {@code AutoCloseable} (since 0.10.1), so javac emits a synthetic {@code close()}
 * call in the generated finally region, which is detected like any other close.
 */
public final class StreamsNotClosedRule implements Rule {

    private static final Set<String> STREAMS_OWNERS = Set.of(KafkaTypes.KAFKA_STREAMS);

    private final Severity severity;

    public StreamsNotClosedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_NOT_CLOSED;
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
        Map<Integer, AbstractInsnNode> streamsSlots = new HashMap<>();
        Set<Integer> usedSlots = new HashSet<>();
        Set<Integer> closedSlots = new HashSet<>();
        Set<Integer> escapedSlots = new HashSet<>();

        for (AbstractInsnNode insn : mn.instructions) {
            if (insn instanceof MethodInsnNode ctor
                    && ctor.getOpcode() == Opcodes.INVOKESPECIAL
                    && "<init>".equals(ctor.name)
                    && KafkaTypes.KAFKA_STREAMS.equals(ctor.owner)) {
                AbstractInsnNode next = AsmUtil.nextSignificant(ctor);
                if (next instanceof VarInsnNode v && v.getOpcode() == Opcodes.ASTORE) {
                    streamsSlots.put(v.var, ctor);
                }
                continue;
            }

            int op = insn.getOpcode();
            if (op == Opcodes.PUTFIELD || op == Opcodes.PUTSTATIC || op == Opcodes.ARETURN) {
                AbstractInsnNode prev = AsmUtil.prevSignificant(insn);
                if (prev instanceof VarInsnNode v && v.getOpcode() == Opcodes.ALOAD
                        && streamsSlots.containsKey(v.var)) {
                    escapedSlots.add(v.var);
                }
                continue;
            }

            // Lambda / method-reference capture: ALOAD slot consumed by an INVOKEDYNAMIC
            // (e.g. `streams::close` shutdown hook, `() -> streams.close()`). The slot
            // is in the wild; the lambda body may close it — mark ESCAPED.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                escapedSlots.addAll(AsmUtil.indyCapturedSlots(indy, streamsSlots.keySet()));
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!STREAMS_OWNERS.contains(mi.owner)) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, streamsSlots.keySet());
            if (slot == null) continue;

            if (mi.name.startsWith("close")) {
                closedSlots.add(slot);
            } else {
                usedSlots.add(slot);
            }
        }

        for (Map.Entry<Integer, AbstractInsnNode> e : streamsSlots.entrySet()) {
            int slot = e.getKey();
            if (!usedSlots.contains(slot)) continue;
            if (closedSlots.contains(slot)) continue;
            if (escapedSlots.contains(slot)) continue;
            out.add(new Violation(
                    RuleId.STREAMS_NOT_CLOSED, severity,
                    ctx.classNode().name, mn.name, AsmUtil.lineOf(e.getValue()),
                    "KafkaStreams is constructed and used (start/setStateListener/...) in this method "
                            + "but close() is never called on the same local slot. close() is the only "
                            + "orderly-shutdown path: without it, JVM exit kills the StreamThreads mid-batch, "
                            + "the consumer-group membership is never released, and the broker keeps the "
                            + "session alive until session.timeout.ms (45 s default in Streams 3.x). "
                            + "Downstream partition reassignment to a healthy peer is delayed by that "
                            + "amount on every restart; lag spikes for the duration. For EOS topologies, "
                            + "the in-flight transaction stays open until transaction.timeout.ms "
                            + "(default 60 s), stalling read_committed consumers downstream. Wrap the "
                            + "KafkaStreams instance in try-with-resources (it implements AutoCloseable "
                            + "since 0.10.1) or register a shutdown hook calling streams.close(Duration)."));
        }
    }
}
