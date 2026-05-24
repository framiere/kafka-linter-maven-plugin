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
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Flags {@code Producer.send(...)} called on a TRANSACTIONAL KafkaProducer slot
 * (one that has had {@code initTransactions()} called on it in the same method)
 * when the slot is NOT currently inside a {@code beginTransaction() → commit/abort}
 * bracket. The producer's {@code TransactionManager.maybeAddPartitionToTransaction()}
 * check throws
 * {@code IllegalStateException("Cannot add partition <tp> to transaction since the
 * transaction is not in progress, set currentState= READY.")} immediately, before
 * any RPC, and the record is never sent.
 *
 * <p>Per-method state machine that combines the slot tracking of
 * {@link ProducerInitTransactionsTwiceRule} (to identify which slots are
 * transactional) with the begin/commit/abort state of
 * {@link ProducerEndTransactionNotInTransactionRule} (to know whether a
 * transaction is currently open). A {@code send} on a non-transactional slot
 * (no init in this method) is silent — the producer is in an idempotent or
 * vanilla mode where send-outside-transaction is the only legal shape.
 *
 * <h2>False-negative envelope</h2>
 *
 * <p>The detector walks bytecode in linear order, not control-flow order. A
 * conditional begin paired with an unconditional send (where the begin's
 * branch is the false path at runtime) appears as a static add to
 * {@code beganSlots} followed by a send that sees the slot in beganSlots, so
 * no violation fires — but at runtime the send executes on a producer that is
 * still in READY. Catching this would require control-flow analysis on
 * {@code mn.tryCatchBlocks} and branch nodes; it is an acknowledged
 * false-negative kept to preserve HIGH confidence on the straight-line shapes
 * the rule actually targets.
 */
public final class ProducerSendOutsideTransactionRule implements Rule {

    private static final String INIT_TRANSACTIONS = "initTransactions";
    private static final String BEGIN_TRANSACTION = "beginTransaction";
    private static final String COMMIT_TRANSACTION = "commitTransaction";
    private static final String ABORT_TRANSACTION = "abortTransaction";
    private static final String SEND = "send";
    private static final String VOID_NO_ARG_DESC = "()V";

    private final Severity severity;

    public ProducerSendOutsideTransactionRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_SEND_OUTSIDE_TRANSACTION;
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
        Set<Integer> initedSlots = new HashSet<>();
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

            // (2) INVOKEDYNAMIC: track method-reference captures that move slot state.
            //     - ::initTransactions  → adds the slot to initedSlots (the slot is
            //       now considered transactional for the rest of the method).
            //     - ::beginTransaction  → adds the slot to beganSlots.
            //     - ::commitTransaction / ::abortTransaction → removes the slot from
            //       beganSlots (mirrors the runtime state-machine: terminal edges
            //       out of IN_TRANSACTION).
            //     - ::send capture on an initialized slot with no open transaction
            //       at capture time fires (the deferred-run will throw).
            if (insn instanceof InvokeDynamicInsnNode indy) {
                if (AsmUtil.indyTargetHandle(indy, KafkaTypes.PRODUCER_OWNERS, INIT_TRANSACTIONS, VOID_NO_ARG_DESC) != null) {
                    initedSlots.addAll(AsmUtil.indyCapturedSlots(indy, producerSlots));
                    continue;
                }
                if (AsmUtil.indyTargetHandle(indy, KafkaTypes.PRODUCER_OWNERS, BEGIN_TRANSACTION, VOID_NO_ARG_DESC) != null) {
                    beganSlots.addAll(AsmUtil.indyCapturedSlots(indy, producerSlots));
                    continue;
                }
                if (AsmUtil.indyTargetHandle(indy, KafkaTypes.PRODUCER_OWNERS, COMMIT_TRANSACTION, null) != null
                        || AsmUtil.indyTargetHandle(indy, KafkaTypes.PRODUCER_OWNERS, ABORT_TRANSACTION, VOID_NO_ARG_DESC) != null) {
                    beganSlots.removeAll(AsmUtil.indyCapturedSlots(indy, producerSlots));
                    continue;
                }
                if (AsmUtil.indyTargetHandle(indy, KafkaTypes.PRODUCER_OWNERS, SEND, null) != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, producerSlots);
                    for (Integer slot : captured) {
                        if (initedSlots.contains(slot) && !beganSlots.contains(slot)) {
                            out.add(captureViolation(ctx, mn, insn));
                        }
                    }
                }
                continue;
            }

            // (3) Direct calls on Producer/KafkaProducer
            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;

            String name = mi.name;
            boolean isInit = INIT_TRANSACTIONS.equals(name) && VOID_NO_ARG_DESC.equals(mi.desc);
            boolean isBegin = BEGIN_TRANSACTION.equals(name) && VOID_NO_ARG_DESC.equals(mi.desc);
            boolean isCommit = COMMIT_TRANSACTION.equals(name); // any overload
            boolean isAbort = ABORT_TRANSACTION.equals(name) && VOID_NO_ARG_DESC.equals(mi.desc);
            boolean isSend = SEND.equals(name); // (record) and (record, callback)
            if (!isInit && !isBegin && !isCommit && !isAbort && !isSend) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, producerSlots);
            if (slot == null) continue;

            if (isInit) {
                initedSlots.add(slot);
                continue;
            }
            if (isBegin) {
                beganSlots.add(slot);
                continue;
            }
            if (isCommit || isAbort) {
                beganSlots.remove(slot);
                continue;
            }
            // isSend
            if (initedSlots.contains(slot) && !beganSlots.contains(slot)) {
                out.add(directViolation(ctx, mn, insn));
            }
        }
    }

    private Violation directViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.PRODUCER_SEND_OUTSIDE_TRANSACTION, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "send() called on a transactional KafkaProducer (initTransactions() was "
                        + "called on this slot earlier in the method) with no matching active "
                        + "beginTransaction() — either send before any begin, send after a "
                        + "commit/abort with no new begin, or send between abort and re-begin. "
                        + "The TransactionManager is in READY (not IN_TRANSACTION); the call "
                        + "throws IllegalStateException(\"Cannot add partition <tp> to "
                        + "transaction since the transaction is not in progress, set "
                        + "currentState= READY.\") inside TransactionManager.maybeAddPartition "
                        + "BEFORE any RPC, the returned Future fails synchronously, and no "
                        + "record reaches the broker. The common origins are a finally-block "
                        + "'safety send' after commit, a conditional begin paired with an "
                        + "unconditional send, or a refactor that moved send() outside the "
                        + "begin/commit bracket. Either wrap the send() in a "
                        + "beginTransaction()/commitTransaction() bracket, or remove the "
                        + "transactional configuration if you do not need atomicity.");
    }

    private Violation captureViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.PRODUCER_SEND_OUTSIDE_TRANSACTION, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "send reference captured on a transactional KafkaProducer that has no "
                        + "matching active beginTransaction() at the capture site. When the "
                        + "captured functional interface runs (on the executor / async thread), "
                        + "the TransactionManager is not in IN_TRANSACTION and the call throws "
                        + "IllegalStateException(\"Cannot add partition <tp> to transaction "
                        + "since the transaction is not in progress\"). The executor's "
                        + "uncaught-exception handler typically swallows the exception and the "
                        + "record is silently dropped. Either remove the orphan capture, or "
                        + "ensure begin/send/commit are all bracketed on the same thread.");
    }
}
