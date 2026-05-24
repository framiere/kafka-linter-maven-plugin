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
 * Flags {@code KafkaStreams.start()} called more than once on the same local
 * slot in the same method. {@code start()} is single-shot by contract — the
 * second call throws {@code IllegalStateException("Streams is not in CREATED
 * state, cannot start.")} because the instance has already transitioned out
 * of the CREATED state.
 *
 * <p>Method-scoped detection on the same slot-tracking infrastructure as
 * {@link StreamsUsedAfterCloseRule}: track {@code new KafkaStreams + ASTORE},
 * walk instructions, fire on the SECOND start() resolving to a tracked slot
 * already in {@code startedSlots}. Method-reference captures
 * ({@code executor.submit(streams::start)}) are also tracked: a second
 * capture of {@code ::start} on the same slot fires.
 */
public final class StreamsStartedTwiceRule implements Rule {

    private final Severity severity;

    public StreamsStartedTwiceRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_STARTED_TWICE;
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
        Set<Integer> streamsSlots = new HashSet<>();
        Set<Integer> startedSlots = new HashSet<>();

        for (AbstractInsnNode insn : mn.instructions) {
            // (1) Track KafkaStreams constructions: new + <init> + astore N
            if (insn instanceof MethodInsnNode ctor
                    && ctor.getOpcode() == Opcodes.INVOKESPECIAL
                    && "<init>".equals(ctor.name)
                    && KafkaTypes.KAFKA_STREAMS.equals(ctor.owner)) {
                AbstractInsnNode next = AsmUtil.nextSignificant(ctor);
                if (next instanceof VarInsnNode v && v.getOpcode() == Opcodes.ASTORE) {
                    streamsSlots.add(v.var);
                }
                continue;
            }

            // (2) Method-reference capture of KafkaStreams.start:
            //     INVOKEDYNAMIC whose bsmArgs contain a REF_invokeVirtual
            //     KafkaStreams.start handle. A second capture (or a capture
            //     after a direct start) on the same slot will throw at
            //     invocation time on the executor / async thread.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                Handle h = AsmUtil.indyTargetHandle(indy, KafkaTypes.STREAMS_OWNERS, "start", null);
                if (h == null) continue;
                Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, streamsSlots);
                for (Integer slot : captured) {
                    if (startedSlots.contains(slot)) {
                        out.add(new Violation(
                                RuleId.STREAMS_STARTED_TWICE, severity,
                                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                                "start() reference captured on a KafkaStreams that was already started "
                                        + "earlier in this method. KafkaStreams.start() is single-shot — "
                                        + "the second invocation (when the captured functional interface "
                                        + "runs on an executor / async thread) throws "
                                        + "IllegalStateException(\"Streams is not in CREATED state, cannot "
                                        + "start.\") because the instance has already transitioned out of "
                                        + "CREATED. The executor's uncaught-exception handler typically "
                                        + "swallows the exception, leaving the operator unaware that the "
                                        + "'restart' attempt failed. If you need to restart, construct a "
                                        + "fresh KafkaStreams instance after close() on the old one."));
                        break;
                    } else {
                        startedSlots.add(slot);
                    }
                }
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.STREAMS_OWNERS.contains(mi.owner)) continue;
            if (!"start".equals(mi.name)) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, streamsSlots);
            if (slot == null) continue;

            if (startedSlots.contains(slot)) {
                out.add(new Violation(
                        RuleId.STREAMS_STARTED_TWICE, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "start() called on a KafkaStreams that was already started earlier in this "
                                + "method. KafkaStreams.start() is single-shot — the second call throws "
                                + "IllegalStateException(\"Streams is not in CREATED state, cannot "
                                + "start.\") because the instance has already transitioned out of CREATED. "
                                + "There is no documented path back to CREATED from any other state, even "
                                + "NOT_RUNNING after close(). If you need to restart, construct a fresh "
                                + "KafkaStreams instance (after close() on the old one and optionally "
                                + "cleanUp() to wipe state-store contents)."));
            } else {
                startedSlots.add(slot);
            }
        }
    }
}
