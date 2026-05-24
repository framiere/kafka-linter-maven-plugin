package io.conductor.kafkalinter.rules.streams;

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
 * Flags {@code KafkaStreams.cleanUp()} called on a started-and-not-yet-closed
 * KafkaStreams instance on the same local slot in the same method.
 * {@code cleanUp()} is documented as legal ONLY before {@code start()} or
 * AFTER {@code close()} — calling it on a running instance throws
 * {@code IllegalStateException("Cannot clean up while running.")} before any
 * state-store deletion happens, so the developer's intent ('reset state') is
 * silently defeated.
 *
 * <p>Method-scoped detection on the same slot-tracking infrastructure as
 * {@link io.conductor.kafkalinter.rules.clients.StreamsSetListenerAfterStartRule}
 * and {@code StreamsStartedTwiceRule}: track {@code new KafkaStreams + ASTORE},
 * walk instructions, maintain a per-method {@code startedSlots} set.
 *
 * <ul>
 *   <li>{@code start()} adds the receiver slot to {@code startedSlots}.</li>
 *   <li>{@code close()} / {@code close(Duration)} / {@code close(CloseOptions)}
 *       REMOVES the receiver slot from {@code startedSlots} — cleanUp() is
 *       legal again after close, per the Javadoc 'May only be called either
 *       before this instance is started or after the instance is closed.'</li>
 *   <li>{@code cleanUp()} on a slot in {@code startedSlots} fires.</li>
 *   <li>{@code INVOKEDYNAMIC} captures of {@code ::start} mark the captured
 *       slot as started; {@code ::close} unmarks it; a {@code ::cleanUp}
 *       capture whose slot is in {@code startedSlots} fires at the indy
 *       site (because by the time the captured Runnable runs on a worker
 *       thread, the instance has been started).</li>
 * </ul>
 *
 * <p>cleanUp() called BEFORE start() (slot not yet in {@code startedSlots})
 * is silent — the canonical correct shape. cleanUp() called AFTER close()
 * (slot removed from {@code startedSlots}) is also silent — the second
 * Javadoc-blessed window. Only the runtime-illegal post-start, pre-close
 * window fires.
 */
public final class StreamsCleanupAfterStartRule implements Rule {

    private static final String START = "start";
    private static final String CLEAN_UP = "cleanUp";
    private static final String CLOSE = "close";
    private static final String VOID_NO_ARG_DESC = "()V";

    private final Severity severity;

    public StreamsCleanupAfterStartRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_CLEANUP_AFTER_START;
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
            // (1) Track KafkaStreams constructions: <init> + ASTORE N
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

            // (2) INVOKEDYNAMIC captures.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                // ::start capture marks the slot as started.
                Handle startH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.STREAMS_OWNERS, START, VOID_NO_ARG_DESC);
                if (startH != null) {
                    startedSlots.addAll(AsmUtil.indyCapturedSlots(indy, streamsSlots));
                    continue;
                }
                // ::close capture (any close overload — name match only) removes
                // the slot from startedSlots — cleanUp is legal again after close.
                Handle closeH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.STREAMS_OWNERS, CLOSE, null);
                if (closeH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, streamsSlots);
                    startedSlots.removeAll(captured);
                    continue;
                }
                // ::cleanUp capture fires if the captured slot is already
                // in startedSlots (i.e. the capture is scheduled to run on a
                // worker thread AFTER start, and there has been no
                // intervening close on the same slot in this method).
                Handle cleanupH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.STREAMS_OWNERS, CLEAN_UP, VOID_NO_ARG_DESC);
                if (cleanupH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, streamsSlots);
                    for (Integer slot : captured) {
                        if (startedSlots.contains(slot)) {
                            out.add(captureViolation(ctx, mn, insn));
                            break;
                        }
                    }
                }
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.STREAMS_OWNERS.contains(mi.owner)) continue;

            String name = mi.name;
            boolean isStart = START.equals(name) && VOID_NO_ARG_DESC.equals(mi.desc);
            boolean isClose = CLOSE.equals(name);
            boolean isCleanUp = CLEAN_UP.equals(name) && VOID_NO_ARG_DESC.equals(mi.desc);
            if (!isStart && !isClose && !isCleanUp) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, streamsSlots);
            if (slot == null) continue;

            if (isStart) {
                startedSlots.add(slot);
                continue;
            }
            if (isClose) {
                startedSlots.remove(slot);
                continue;
            }
            // isCleanUp
            if (startedSlots.contains(slot)) {
                out.add(directViolation(ctx, mn, insn));
            }
        }
    }

    private Violation directViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_CLEANUP_AFTER_START, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KafkaStreams.cleanUp() called on a started instance with no intervening close() "
                        + "in this method. cleanUp() is documented as legal only BEFORE start() "
                        + "(while the instance is in CREATED) or AFTER close() (while the instance "
                        + "is in NOT_RUNNING). On a running instance the call throws "
                        + "IllegalStateException(\"Cannot clean up while running.\") before any "
                        + "state-store deletion happens — so the developer's intent ('reset state "
                        + "before continuing') is silently defeated: state.dir on disk is "
                        + "unchanged, the next restart still replays from the SAME state stores, "
                        + "and the bug the cleanUp() was meant to recover from re-occurs on the "
                        + "next record. The exception fires on the calling thread but is "
                        + "typically caught by a generic try/catch around 'startup' or 'reset' "
                        + "code, logged as a single 'failed to reset state' warning, and "
                        + "forgotten. Fix: move the cleanUp() call to BEFORE start() in the same "
                        + "method (canonical pre-start reset), or to AFTER close() (managed reset "
                        + "before reconstruct). If you need to reset state mid-flight, the "
                        + "correct sequence is close() → cleanUp() → construct a fresh "
                        + "KafkaStreams → start() — the cleanUp() is on the OLD instance after "
                        + "its close, not on the new instance after its start.");
    }

    private Violation captureViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_CLEANUP_AFTER_START, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "cleanUp() reference captured (e.g. executor.submit(streams::cleanUp), "
                        + "executor.schedule(streams::cleanUp, ...), CompletableFuture.runAsync("
                        + "streams::cleanUp)) on a KafkaStreams that has already been started "
                        + "earlier in this method with no intervening close(). When the captured "
                        + "Runnable later runs on the worker thread, cleanUp() throws "
                        + "IllegalStateException(\"Cannot clean up while running.\") because the "
                        + "instance is no longer in CREATED nor in NOT_RUNNING. The executor's "
                        + "uncaught-exception handler typically swallows the exception; the "
                        + "scheduled maintenance / reset is a silent no-op for the lifetime of "
                        + "the process. Fix: move the cleanUp() to BEFORE start() in the same "
                        + "method (canonical pre-start reset), or to AFTER close() (managed reset "
                        + "before reconstruct). cleanUp() must never be scheduled to run on an "
                        + "instance that is or will be RUNNING.");
    }
}
