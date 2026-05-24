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
 * Flags {@code Producer.beginTransaction()} called twice on the same local slot
 * in the same method without an intervening {@code commitTransaction()} /
 * {@code abortTransaction()}. The producer's {@code TransactionManager} only
 * allows the {@code READY → IN_TRANSACTION} edge via {@code beginTransaction()};
 * the second call executes against a manager already in {@code IN_TRANSACTION}
 * and throws {@code KafkaException("Invalid transition attempted from state
 * IN_TRANSACTION to state IN_TRANSACTION")} before any RPC.
 *
 * <p>Method-scoped detection on the same slot-tracking infrastructure as
 * {@link StreamsStartedTwiceRule} and {@link ConsumerSubscribeAndAssignMixedRule}:
 * track {@code new KafkaProducer + ASTORE}, walk instructions, maintain a
 * per-method {@code beganSlots} set. A second {@code beginTransaction()} on a
 * slot already in {@code beganSlots} fires; {@code commitTransaction()} and
 * {@code abortTransaction()} remove the slot (drive the model state machine
 * back to READY). Method-reference captures
 * ({@code executor.submit(producer::beginTransaction)}) are also tracked.
 *
 * <h2>Catch-handler false-negative</h2>
 *
 * <p>The detector walks bytecode in linear order, not control-flow order, so
 * a shape like {@code try { begin; send; commit; } catch (Exception e) { begin; ... }}
 * is NOT reported: linearly, the {@code commit} appears before the second
 * {@code begin}, so the model state machine is back to READY when the second
 * begin is reached. Runtime control flow is different — the catch handler
 * runs only when the {@code commit} (or any prior step) threw, so the
 * manager is actually still in {@code IN_TRANSACTION} when the second begin
 * executes. Catching this would require control-flow analysis on
 * {@code mn.tryCatchBlocks}; it is an acknowledged false-negative kept to
 * preserve HIGH confidence on the straight-line and method-reference shapes
 * the rule actually targets. The companion rule
 * {@link ProducerBeginTransactionNoAbortRule} catches the catch-handler
 * shape from a different angle (begin without abort anywhere in the same
 * method).
 */
public final class ProducerBeginTransactionTwiceRule implements Rule {

    private static final String BEGIN_TRANSACTION = "beginTransaction";
    private static final String COMMIT_TRANSACTION = "commitTransaction";
    private static final String ABORT_TRANSACTION = "abortTransaction";
    private static final String VOID_NO_ARG_DESC = "()V";

    private final Severity severity;

    public ProducerBeginTransactionTwiceRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_BEGIN_TRANSACTION_TWICE;
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

            // (2) Method-reference captures: indy bsmArgs containing a
            //     REF_invoke* handle for beginTransaction / commitTransaction /
            //     abortTransaction. A captured ::beginTransaction on a slot
            //     already in beganSlots fires at the indy site — when the
            //     executor / async stage runs the captured ref, the second
            //     begin throws KafkaException on the worker thread.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                Handle beginH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.PRODUCER_OWNERS, BEGIN_TRANSACTION, VOID_NO_ARG_DESC);
                if (beginH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, producerSlots);
                    for (Integer slot : captured) {
                        if (beganSlots.contains(slot)) {
                            out.add(new Violation(
                                    RuleId.PRODUCER_BEGIN_TRANSACTION_TWICE, severity,
                                    ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                                    "beginTransaction() reference captured on a KafkaProducer that "
                                            + "had already entered IN_TRANSACTION earlier in this "
                                            + "method. Producer.beginTransaction() requires the "
                                            + "TransactionManager to be in READY — when the captured "
                                            + "functional interface runs (on the executor / async "
                                            + "thread), the call throws KafkaException(\"Invalid "
                                            + "transition attempted from state IN_TRANSACTION to "
                                            + "state IN_TRANSACTION\"). The executor's "
                                            + "uncaught-exception handler typically swallows the "
                                            + "exception, the second transaction never opens, and "
                                            + "the first transaction stays open — read_committed "
                                            + "consumers downstream stall behind the Last Stable "
                                            + "Offset until transaction.timeout.ms elapses. Call "
                                            + "commitTransaction() or abortTransaction() between "
                                            + "the two begins, or remove the duplicate begin."));
                        } else {
                            beganSlots.add(slot);
                        }
                    }
                    continue;
                }
                Handle commitH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.PRODUCER_OWNERS, COMMIT_TRANSACTION, VOID_NO_ARG_DESC);
                if (commitH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, producerSlots);
                    beganSlots.removeAll(captured);
                    continue;
                }
                Handle abortH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.PRODUCER_OWNERS, ABORT_TRANSACTION, VOID_NO_ARG_DESC);
                if (abortH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, producerSlots);
                    beganSlots.removeAll(captured);
                }
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;
            if (!VOID_NO_ARG_DESC.equals(mi.desc)) continue;

            String name = mi.name;
            boolean isBegin = BEGIN_TRANSACTION.equals(name);
            boolean isCommit = COMMIT_TRANSACTION.equals(name);
            boolean isAbort = ABORT_TRANSACTION.equals(name);
            if (!isBegin && !isCommit && !isAbort) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, producerSlots);
            if (slot == null) continue;

            if (isCommit || isAbort) {
                beganSlots.remove(slot);
                continue;
            }

            // isBegin
            if (beganSlots.contains(slot)) {
                out.add(new Violation(
                        RuleId.PRODUCER_BEGIN_TRANSACTION_TWICE, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "beginTransaction() called on a KafkaProducer that had already entered "
                                + "IN_TRANSACTION earlier in this method without an intervening "
                                + "commitTransaction()/abortTransaction(). Producer.beginTransaction() "
                                + "requires the TransactionManager to be in READY — the second begin "
                                + "throws KafkaException(\"Invalid transition attempted from state "
                                + "IN_TRANSACTION to state IN_TRANSACTION\") before any RPC, the "
                                + "second transaction never opens, and the FIRST transaction is "
                                + "still open with all of its sends and offsets in limbo. "
                                + "read_committed consumers downstream of every partition the first "
                                + "transaction wrote to stall behind the Last Stable Offset until "
                                + "transaction.timeout.ms elapses (default 60 s for plain producers, "
                                + "10 min for Streams). Either remove the duplicate begin, or insert "
                                + "commitTransaction()/abortTransaction() between the two begins to "
                                + "drive the manager back to READY first."));
            } else {
                beganSlots.add(slot);
            }
        }
    }
}
