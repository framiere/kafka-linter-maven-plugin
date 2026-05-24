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
 * Flags {@code Producer.commitTransaction()} or {@code Producer.abortTransaction()}
 * called on the same local slot in the same method WITHOUT a matching unbalanced
 * {@code beginTransaction()} preceding it. The producer's {@code TransactionManager}
 * only allows the {@code IN_TRANSACTION → READY} edge via the commit/abort handshake;
 * an end-transaction call against any other state (READY, UNINITIALIZED, INITIALIZING,
 * COMMITTING_TRANSACTION, ABORTING_TRANSACTION, FATAL_ERROR) throws
 * {@code KafkaException("Invalid transition attempted from state X to state
 * COMMITTING_TRANSACTION")} (or {@code ABORTING_TRANSACTION}) before any RPC.
 *
 * <p>Method-scoped detection that mirrors {@link ProducerBeginTransactionTwiceRule} —
 * same slot-tracking infrastructure, same {@code beganSlots} model state machine —
 * with the firing condition inverted: begin ADDS to the set; commit/abort fire if
 * the slot is NOT in the set, and remove the slot otherwise. {@code commitTransaction}
 * is matched by name (covering the {@code ()V} overload, the
 * {@code (Map<TopicPartition, OffsetAndMetadata>, ConsumerGroupMetadata)} EOS overload,
 * and the deprecated {@code (Map, String)} overload — all share the same IN_TRANSACTION
 * precondition). Method-reference captures via {@code INVOKEDYNAMIC} are tracked
 * symmetrically (capture-site signal is what the rule keys off of).
 *
 * <h2>False-negative envelope</h2>
 *
 * <p>Identical to {@link ProducerBeginTransactionTwiceRule}: the detector walks
 * bytecode in linear order, not control-flow order. A shape like {@code try {
 * begin; send; commit; } catch (Exception e) { commit; }} is NOT reported because
 * linearly, the first commit removes the slot from {@code beganSlots}, then the
 * catch-block commit sees an empty set and fires — that IS reported. But a shape
 * like {@code if (cond) { begin; doStuff; } commit;} where {@code cond} is false at
 * runtime: the static analysis sees the begin add to beganSlots and the commit
 * remove it, so neither fires statically — at runtime the begin is skipped and the
 * commit throws. Catching this would require control-flow analysis on
 * {@code mn.tryCatchBlocks} and branch nodes; it is an acknowledged false-negative
 * kept to preserve HIGH confidence on the straight-line shapes the rule actually
 * targets. The companion rule {@link ProducerBeginTransactionNoAbortRule} catches
 * the orthogonal 'begin without abort in any code path' shape.
 */
public final class ProducerEndTransactionNotInTransactionRule implements Rule {

    private static final String BEGIN_TRANSACTION = "beginTransaction";
    private static final String COMMIT_TRANSACTION = "commitTransaction";
    private static final String ABORT_TRANSACTION = "abortTransaction";
    private static final String VOID_NO_ARG_DESC = "()V";

    private final Severity severity;

    public ProducerEndTransactionNotInTransactionRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_END_TRANSACTION_NOT_IN_TRANSACTION;
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
            //     ::abortTransaction fires if not in beganSlots, else removes.
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
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, producerSlots);
                    for (Integer slot : captured) {
                        if (!beganSlots.contains(slot)) {
                            out.add(captureViolation(ctx, mn, insn, /*commit*/ true));
                        } else {
                            beganSlots.remove(slot);
                        }
                    }
                    continue;
                }
                Handle abortH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.PRODUCER_OWNERS, ABORT_TRANSACTION, VOID_NO_ARG_DESC);
                if (abortH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, producerSlots);
                    for (Integer slot : captured) {
                        if (!beganSlots.contains(slot)) {
                            out.add(captureViolation(ctx, mn, insn, /*commit*/ false));
                        } else {
                            beganSlots.remove(slot);
                        }
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
            if (!isBegin && !isCommit && !isAbort) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, producerSlots);
            if (slot == null) continue;

            if (isBegin) {
                beganSlots.add(slot);
                continue;
            }

            // isCommit or isAbort
            if (!beganSlots.contains(slot)) {
                out.add(directViolation(ctx, mn, insn, isCommit));
            } else {
                beganSlots.remove(slot);
            }
        }
    }

    private Violation directViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn, boolean isCommit) {
        String verb = isCommit ? "commitTransaction()" : "abortTransaction()";
        String targetState = isCommit ? "COMMITTING_TRANSACTION" : "ABORTING_TRANSACTION";
        return new Violation(
                RuleId.PRODUCER_END_TRANSACTION_NOT_IN_TRANSACTION, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                verb + " called on a KafkaProducer that has no matching active "
                        + "beginTransaction() in this method (the TransactionManager is in "
                        + "READY, not IN_TRANSACTION). Producer." + verb + " requires the manager "
                        + "to be in IN_TRANSACTION — the call throws KafkaException(\"Invalid "
                        + "transition attempted from state READY to state " + targetState + "\") "
                        + "before any RPC and no transaction marker reaches the broker. The "
                        + "common origin is a finally-block 'safety net' commit/abort, a shutdown "
                        + "hook that always aborts, or a conditional begin paired with an "
                        + "unconditional end. Either remove the orphan end-transaction, or "
                        + "ensure a matching beginTransaction() runs unconditionally before it.");
    }

    private Violation captureViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn, boolean isCommit) {
        String verb = isCommit ? "commitTransaction()" : "abortTransaction()";
        String targetState = isCommit ? "COMMITTING_TRANSACTION" : "ABORTING_TRANSACTION";
        return new Violation(
                RuleId.PRODUCER_END_TRANSACTION_NOT_IN_TRANSACTION, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                verb + " reference captured on a KafkaProducer that has no matching active "
                        + "beginTransaction() at the capture site. When the captured functional "
                        + "interface runs (on the executor / async thread), the TransactionManager "
                        + "is not in IN_TRANSACTION and the call throws KafkaException(\"Invalid "
                        + "transition attempted from state READY to state " + targetState + "\"). "
                        + "The executor's uncaught-exception handler typically swallows the "
                        + "exception and the 'safety end-transaction' is a silent no-op for the "
                        + "lifetime of the process. Either remove the orphan capture, or gate it "
                        + "on an application-managed 'transaction is open' flag.");
    }
}
