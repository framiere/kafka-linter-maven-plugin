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
 * Flags {@code Producer.sendOffsetsToTransaction(...)} called on a local slot in
 * the same method WITHOUT a matching unbalanced {@code beginTransaction()}
 * preceding it. The producer's {@code TransactionManager} only allows
 * {@code sendOffsetsToTransaction} while currentState is {@code IN_TRANSACTION};
 * any other state (READY, UNINITIALIZED, INITIALIZING, COMMITTING_TRANSACTION,
 * ABORTING_TRANSACTION, FATAL_ERROR) throws
 * {@code KafkaException("Cannot send offsets if a transaction is not in progress
 * (currentState= <state>).")} before any RPC.
 *
 * <p>Method-scoped detection that mirrors {@link ProducerEndTransactionNotInTransactionRule}
 * — same slot-tracking infrastructure, same {@code beganSlots} model state machine —
 * with one CRITICAL difference: {@code sendOffsetsToTransaction} is NOT a terminal
 * transition. The TransactionManager stays in IN_TRANSACTION after the call (the
 * offsets are buffered until the eventual {@code commitTransaction()}), so the
 * rule fires when the slot is NOT in {@code beganSlots} but does NOT remove the
 * slot when the call matches a tracked transaction. Multiple sendOffsets calls
 * inside the same begin/commit bracket are legitimate.
 *
 * <p>{@code sendOffsetsToTransaction} is matched by NAME only — covers both the
 * modern {@code (Map<TopicPartition, OffsetAndMetadata>, ConsumerGroupMetadata)}
 * overload (KIP-447, Kafka 2.5+) and the deprecated {@code (Map, String)}
 * overload — both share the IN_TRANSACTION precondition check.
 *
 * <h2>False-negative envelope</h2>
 *
 * <p>Identical to {@link ProducerEndTransactionNotInTransactionRule}: linear
 * bytecode walk, not control-flow walk. Cross-method analysis and cross-slot
 * aliasing are accepted as false-negatives. A conditional begin paired with an
 * unconditional sendOffsets does NOT fire because the linear walk adds the slot
 * to beganSlots when visiting the begin in the if-body.
 */
public final class ProducerSendOffsetsToTransactionNotInTransactionRule implements Rule {

    private static final String BEGIN_TRANSACTION = "beginTransaction";
    private static final String COMMIT_TRANSACTION = "commitTransaction";
    private static final String ABORT_TRANSACTION = "abortTransaction";
    private static final String SEND_OFFSETS_TO_TRANSACTION = "sendOffsetsToTransaction";
    private static final String VOID_NO_ARG_DESC = "()V";

    private final Severity severity;

    public ProducerSendOffsetsToTransactionNotInTransactionRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_SEND_OFFSETS_TO_TRANSACTION_NOT_IN_TRANSACTION;
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
        Set<Integer> beganSlots = new HashSet<>();

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

            // (2) INVOKEDYNAMIC: ::beginTransaction adds, ::commitTransaction /
            //     ::abortTransaction remove, ::sendOffsetsToTransaction fires if
            //     not in beganSlots (non-terminal — does NOT remove).
            if (insn instanceof InvokeDynamicInsnNode indy) {
                Handle beginH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.PRODUCER_OWNERS, BEGIN_TRANSACTION, VOID_NO_ARG_DESC);
                if (beginH != null) {
                    beganSlots.addAll(AsmUtil.indyCapturedSlots(indy, producerSlots));
                    continue;
                }
                Handle commitH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.PRODUCER_OWNERS, COMMIT_TRANSACTION, null);
                if (commitH != null) {
                    beganSlots.removeAll(AsmUtil.indyCapturedSlots(indy, producerSlots));
                    continue;
                }
                Handle abortH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.PRODUCER_OWNERS, ABORT_TRANSACTION, VOID_NO_ARG_DESC);
                if (abortH != null) {
                    beganSlots.removeAll(AsmUtil.indyCapturedSlots(indy, producerSlots));
                    continue;
                }
                Handle sendOffsetsH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.PRODUCER_OWNERS, SEND_OFFSETS_TO_TRANSACTION, null);
                if (sendOffsetsH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, producerSlots);
                    for (Integer slot : captured) {
                        if (!beganSlots.contains(slot)) {
                            out.add(captureViolation(ctx, mn, insn));
                        }
                        // Do NOT remove: sendOffsets is non-terminal.
                    }
                }
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;

            String name = mi.name;
            boolean isBegin = BEGIN_TRANSACTION.equals(name) && VOID_NO_ARG_DESC.equals(mi.desc);
            boolean isCommit = COMMIT_TRANSACTION.equals(name); // any overload
            boolean isAbort = ABORT_TRANSACTION.equals(name) && VOID_NO_ARG_DESC.equals(mi.desc);
            boolean isSendOffsets = SEND_OFFSETS_TO_TRANSACTION.equals(name); // any overload
            if (!isBegin && !isCommit && !isAbort && !isSendOffsets) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, producerSlots);
            if (slot == null) continue;

            if (isBegin) {
                beganSlots.add(slot);
                continue;
            }
            if (isCommit || isAbort) {
                beganSlots.remove(slot);
                continue;
            }

            // isSendOffsets — non-terminal: fire if not in beganSlots, do NOT remove.
            if (!beganSlots.contains(slot)) {
                out.add(directViolation(ctx, mn, insn));
            }
        }
    }

    private Violation directViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.PRODUCER_SEND_OFFSETS_TO_TRANSACTION_NOT_IN_TRANSACTION, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "sendOffsetsToTransaction() called on a KafkaProducer that has no matching active "
                        + "beginTransaction() in this method (the TransactionManager is in READY, "
                        + "not IN_TRANSACTION). Producer.sendOffsetsToTransaction() requires the "
                        + "manager to be in IN_TRANSACTION — the call throws "
                        + "KafkaException(\"Cannot send offsets if a transaction is not in progress "
                        + "(currentState= READY).\") before any RPC, no offsets reach "
                        + "__consumer_offsets and no transaction marker is written. The common "
                        + "origin is a poll-process-produce loop that puts the offsets-commit "
                        + "BEFORE beginTransaction, AFTER commitTransaction, or in a finally-block "
                        + "'safety net' that runs after the transaction has already terminated. "
                        + "Move the sendOffsetsToTransaction call BETWEEN beginTransaction and the "
                        + "matching commitTransaction on the same slot.");
    }

    private Violation captureViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.PRODUCER_SEND_OFFSETS_TO_TRANSACTION_NOT_IN_TRANSACTION, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "sendOffsetsToTransaction reference captured on a KafkaProducer that has no "
                        + "matching active beginTransaction() at the capture site. When the "
                        + "captured functional interface runs (on the executor / async thread), "
                        + "the TransactionManager is not in IN_TRANSACTION and the call throws "
                        + "KafkaException(\"Cannot send offsets if a transaction is not in "
                        + "progress (currentState= READY).\"). The executor's uncaught-exception "
                        + "handler typically swallows the exception and the 'offset commit' is a "
                        + "silent no-op. Either remove the orphan capture, or capture the "
                        + "reference between a matching beginTransaction and commitTransaction.");
    }
}
