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
 * Flags a KafkaStreams-lifecycle call (start/store/pause/addStreamThread/setUncaughtExceptionHandler/...)
 * made on a local slot AFTER {@code close()} was called on the same slot in the same method.
 *
 * <p>Direct mirror of {@link ProducerUsedAfterCloseRule} and {@link ConsumerUsedAfterCloseRule}:
 * same detection state machine, same backward stack-effect simulation for receiver resolution,
 * same method-reference capture handling via {@code INVOKEDYNAMIC}. KafkaStreams uses the
 * constructor pattern (not a static factory), so the slot-registration shape matches the
 * producer / consumer case: {@code INVOKESPECIAL <init>} on {@code KafkaStreams} followed
 * by {@code ASTORE N}.
 *
 * <p>{@code cleanUp()} is deliberately excluded from the lifecycle set — its Javadoc states
 * that it is callable in {@code NOT_RUNNING} state (i.e. after close), and applications
 * legitimately call {@code close()} → {@code cleanUp()} to wipe state-store contents before
 * a fresh topology run.
 */
public final class StreamsUsedAfterCloseRule implements Rule {

    private static final Set<String> LIFECYCLE_METHODS = Set.of(
            // lifecycle transitions
            "start",
            "pause", "resume",
            "addStreamThread", "removeStreamThread",
            // listeners — documented as CREATED-state-only, so a fortiori illegal after close
            "setUncaughtExceptionHandler",
            "setStateListener",
            "setGlobalStateRestoreListener",
            "setStandbyUpdateListener",
            // interactive queries
            "store",
            "allLocalStorePartitionLags",
            "queryMetadataForKey",
            "allMetadata", "allMetadataForStore",
            "streamsMetadataForStore");

    private final Severity severity;

    public StreamsUsedAfterCloseRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_USED_AFTER_CLOSE;
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
        Set<Integer> closedSlots = new HashSet<>();

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

            // (2) Method-reference capture of a streams lifecycle method:
            //     INVOKEDYNAMIC whose bsmArgs contain a REF_invokeVirtual
            //     KafkaStreams.<lifecycle>(...) handle. Treat it as a use of
            //     the captured slot: if the slot is already closed, the deferred
            //     call will throw IllegalStateException when the captured
            //     functional interface is invoked on an executor / async thread.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                Handle h = AsmUtil.indyTargetHandle(indy, KafkaTypes.STREAMS_OWNERS, null, null);
                if (h == null) continue;
                if (!LIFECYCLE_METHODS.contains(h.getName())) continue;
                Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, streamsSlots);
                for (Integer slot : captured) {
                    if (closedSlots.contains(slot)) {
                        out.add(new Violation(
                                RuleId.STREAMS_USED_AFTER_CLOSE, severity,
                                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                                h.getName() + "(...) reference captured on a KafkaStreams that was already "
                                        + "closed earlier in this method. close() drives the instance to "
                                        + "NOT_RUNNING — when the deferred invocation runs (typically on an "
                                        + "executor or async thread) it throws IllegalStateException (or, "
                                        + "for IQ methods, returns null / throws InvalidStateStoreException); "
                                        + "in an async hand-off the exception is silently swallowed and the "
                                        + "operation never actually runs. Move close() to the very last call "
                                        + "on the streams instance (cleanUp() is the only documented "
                                        + "exception — safe in NOT_RUNNING)."));
                        break;
                    }
                }
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.STREAMS_OWNERS.contains(mi.owner)) continue;

            if (mi.name.startsWith("close")) {
                Integer slot = AsmUtil.resolveReceiverSlot(mi, streamsSlots);
                if (slot != null) closedSlots.add(slot);
                continue;
            }

            if (!LIFECYCLE_METHODS.contains(mi.name)) continue;
            Integer slot = AsmUtil.resolveReceiverSlot(mi, streamsSlots);
            if (slot == null || !closedSlots.contains(slot)) continue;

            out.add(new Violation(
                    RuleId.STREAMS_USED_AFTER_CLOSE, severity,
                    ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                    mi.name + "(...) called on a KafkaStreams that was already closed earlier "
                            + "in this method. close() drives the instance to NOT_RUNNING — any "
                            + "subsequent call throws IllegalStateException (or, for IQ methods, returns "
                            + "null / throws InvalidStateStoreException); in an async hand-off the "
                            + "exception is silently swallowed and the operation never actually runs. "
                            + "Move close() to the very last call on the streams instance (cleanUp() "
                            + "is the only documented exception — safe in NOT_RUNNING)."));
        }
    }
}
