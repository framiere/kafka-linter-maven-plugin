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
 * Flags {@code Producer.initTransactions()} called more than once on the same
 * local slot in the same method. {@code initTransactions()} is a one-shot
 * lifecycle entry: the producer's {@code TransactionManager} state machine
 * only allows the UNINITIALIZED → INITIALIZING transition, so the second call
 * throws
 * {@code KafkaException("TransactionalId <id>: Invalid transition attempted
 * from state READY to state INITIALIZING")} before any RPC.
 *
 * <p>Method-scoped detection that mirrors {@link
 * io.conductor.kafkalinter.rules.clients.ProducerSendOffsetsToTransactionNotInTransactionRule}
 * and the begin/commit/abort sibling family: slot tracking via constructor +
 * ASTORE, receiver resolution via backward stack-effect simulation, and
 * INVOKEDYNAMIC method-reference symmetry. No reset edge — once a slot's first
 * init has been seen, every subsequent init on that slot in the same method
 * fires.
 */
public final class ProducerInitTransactionsTwiceRule implements Rule {

    private static final String INIT_TRANSACTIONS = "initTransactions";
    private static final String VOID_NO_ARG_DESC = "()V";

    private final Severity severity;

    public ProducerInitTransactionsTwiceRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_INIT_TRANSACTIONS_TWICE;
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
        Set<Integer> initSlots = new HashSet<>();

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

            // (2) INVOKEDYNAMIC: ::initTransactions capture. First capture on a
            //     given slot is legitimate (adds to initSlots); every subsequent
            //     capture on the same slot fires.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                Handle initH = AsmUtil.indyTargetHandle(
                        indy, KafkaTypes.PRODUCER_OWNERS, INIT_TRANSACTIONS, VOID_NO_ARG_DESC);
                if (initH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, producerSlots);
                    for (Integer slot : captured) {
                        if (initSlots.contains(slot)) {
                            out.add(captureViolation(ctx, mn, insn));
                        } else {
                            initSlots.add(slot);
                        }
                    }
                }
                continue;
            }

            // (3) Direct calls: producer.initTransactions()
            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) continue;
            if (!INIT_TRANSACTIONS.equals(mi.name)) continue;
            if (!VOID_NO_ARG_DESC.equals(mi.desc)) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, producerSlots);
            if (slot == null) continue;

            if (initSlots.contains(slot)) {
                out.add(directViolation(ctx, mn, insn));
            } else {
                initSlots.add(slot);
            }
        }
    }

    private Violation directViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.PRODUCER_INIT_TRANSACTIONS_TWICE, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "initTransactions() called more than once on the same KafkaProducer slot in "
                        + "this method. initTransactions() is a one-shot lifecycle entry: the "
                        + "internal TransactionManager only allows the UNINITIALIZED → "
                        + "INITIALIZING state transition. The second call runs against a "
                        + "manager in READY (or IN_TRANSACTION / COMMITTING_TRANSACTION / "
                        + "ABORTING_TRANSACTION / FATAL_ERROR) and throws "
                        + "KafkaException(\"TransactionalId <id>: Invalid transition attempted "
                        + "from state READY to state INITIALIZING\") before any RPC. The common "
                        + "origins are an 'idempotent init' retry wrapper, a lifecycle hook that "
                        + "duplicates init across @PostConstruct and an admin endpoint, or a "
                        + "test-harness @BeforeEach that re-inits a producer set up in @BeforeAll. "
                        + "initTransactions() must be called EXACTLY ONCE per producer instance, "
                        + "in the construction path, before any transactional method.");
    }

    private Violation captureViolation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.PRODUCER_INIT_TRANSACTIONS_TWICE, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "initTransactions reference captured on a KafkaProducer that has already had "
                        + "initTransactions() called on it (directly or via an earlier capture) "
                        + "in this method. When the captured functional interface runs (on the "
                        + "executor / async thread), the TransactionManager is in READY (or a "
                        + "later state) and the call throws KafkaException(\"Invalid transition "
                        + "attempted from state READY to state INITIALIZING\"). The executor's "
                        + "uncaught-exception handler typically swallows the exception and the "
                        + "worker thread dies silently. Remove the orphan capture — "
                        + "initTransactions() must run EXACTLY ONCE per producer instance.");
    }
}
