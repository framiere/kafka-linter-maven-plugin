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
 * Flags {@code KafkaStreams.setStateListener} /
 * {@code setUncaughtExceptionHandler} / {@code setGlobalStateRestoreListener} /
 * {@code setStandbyUpdateListener} called AFTER {@code start()} on the same
 * local slot in the same method. All four setters are CREATED-state-only by
 * contract; once {@code start()} has been called, the instance has
 * transitioned out of CREATED, and the setter throws
 * {@code IllegalStateException("Can only set <listener> in CREATED state.
 * Current state is <state>.")} before storing the reference.
 *
 * <p>Method-scoped detection on the same slot-tracking infrastructure as
 * {@link StreamsStartedTwiceRule} and {@link StreamsUsedAfterCloseRule}:
 * track {@code new KafkaStreams + ASTORE}, walk instructions, maintain a
 * per-method {@code startedSlots} set. A listener setter on a slot already
 * in {@code startedSlots} fires; an {@code INVOKEDYNAMIC} capture of
 * {@code ::start} (or the direct call) adds the slot to the set, and an
 * {@code INVOKEDYNAMIC} capture of any listener-setter reference on an
 * already-started slot fires symmetrically.
 *
 * <p>Setter name matching: by NAME ONLY (the descriptor varies because
 * {@code setUncaughtExceptionHandler} has both the legacy
 * {@code java.lang.Thread$UncaughtExceptionHandler} overload — deprecated
 * since Kafka 2.8 — AND the modern
 * {@code org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler}
 * overload, and both are subject to the CREATED-only check).
 */
public final class StreamsSetListenerAfterStartRule implements Rule {

    private static final String START = "start";
    private static final String VOID_NO_ARG_DESC = "()V";

    private static final Set<String> LISTENER_SETTERS = Set.of(
            "setStateListener",
            "setUncaughtExceptionHandler",
            "setGlobalStateRestoreListener",
            "setStandbyUpdateListener");

    private final Severity severity;

    public StreamsSetListenerAfterStartRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_SET_LISTENER_AFTER_START;
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

            // (2) INVOKEDYNAMIC captures.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                // ::start captures mark the slot as started (mirroring the
                // direct-call shape).
                Handle startH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.STREAMS_OWNERS, START, VOID_NO_ARG_DESC);
                if (startH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, streamsSlots);
                    startedSlots.addAll(captured);
                    continue;
                }
                // Listener-setter captures fire if the captured slot is
                // already in startedSlots.
                for (String setter : LISTENER_SETTERS) {
                    Handle setterH = AsmUtil.indyTargetHandle(
                            indy, KafkaTypes.STREAMS_OWNERS, setter, null);
                    if (setterH != null) {
                        Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, streamsSlots);
                        for (Integer slot : captured) {
                            if (startedSlots.contains(slot)) {
                                out.add(new Violation(
                                        RuleId.STREAMS_SET_LISTENER_AFTER_START, severity,
                                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                                        setter + "() reference captured on a KafkaStreams that has "
                                                + "already been started earlier in this method. "
                                                + setter + "() is CREATED-state-only: when the "
                                                + "captured functional interface runs (on the "
                                                + "executor / async thread), the instance is no "
                                                + "longer in CREATED and the setter throws "
                                                + "IllegalStateException(\"Can only set listener in "
                                                + "CREATED state.\"). The executor's "
                                                + "uncaught-exception handler typically swallows the "
                                                + "exception; the listener is NEVER registered; "
                                                + "the application runs in a degraded "
                                                + "no-observability state for the lifetime of the "
                                                + "process. Move the " + setter + "() call to "
                                                + "BEFORE start() in the same method, or to the "
                                                + "construction site."));
                                break;
                            }
                        }
                        break;
                    }
                }
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.STREAMS_OWNERS.contains(mi.owner)) continue;

            String name = mi.name;
            boolean isStart = START.equals(name) && VOID_NO_ARG_DESC.equals(mi.desc);
            boolean isListenerSetter = LISTENER_SETTERS.contains(name);
            if (!isStart && !isListenerSetter) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, streamsSlots);
            if (slot == null) continue;

            if (isStart) {
                startedSlots.add(slot);
                continue;
            }

            // isListenerSetter
            if (startedSlots.contains(slot)) {
                out.add(new Violation(
                        RuleId.STREAMS_SET_LISTENER_AFTER_START, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        name + "() called on a KafkaStreams that has already been started earlier in "
                                + "this method. " + name + "() is CREATED-state-only: once start() "
                                + "has transitioned the instance out of CREATED, the setter throws "
                                + "IllegalStateException(\"Can only set listener in CREATED state. "
                                + "Current state is <state>.\") before storing the reference. The "
                                + "exception fires on the calling thread but is typically caught by "
                                + "a generic try/catch around 'startup' code and logged as a single "
                                + "warning — the listener is NEVER registered, and the application "
                                + "runs with no state listener / no uncaught-exception handler / no "
                                + "restore listener / no standby listener for the lifetime of the "
                                + "process. There is no runtime signal that anything is wrong: no "
                                + "alert fires, no metric moves, no log line repeats. Move the "
                                + name + "() call to BEFORE start() in the same method, or to the "
                                + "construction site if the listener is created externally."));
            }
        }
    }
}
