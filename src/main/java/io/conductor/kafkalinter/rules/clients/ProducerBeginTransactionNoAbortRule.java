package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Fires when a method calls {@code Producer.beginTransaction()} but does not call
 * {@code Producer.abortTransaction()} anywhere in the same method.
 *
 * <h2>Why method scope</h2>
 *
 * <p>The canonical EOS skeleton co-locates begin and abort in the same
 * try/catch — they belong to the same logical transaction boundary. A
 * project-scoped check would silently suppress the rule whenever a different
 * helper somewhere in the project happens to call {@code abortTransaction()}
 * (e.g. a recovery utility used only by tests). Method scope keeps the rule
 * tied to the actual call site that owns the transaction.
 *
 * <h2>Method-reference capture recognised as abort wiring</h2>
 *
 * <p>The catch path sometimes wires the abort via a captured method-reference,
 * e.g. {@code try.recover(producer::abortTransaction)} or
 * {@code executor.execute(producer::abortTransaction)}. javac compiles
 * {@code producer::abortTransaction} to an {@code INVOKEDYNAMIC} whose
 * bootstrap-method args contain a {@code REF_invokeVirtual} /
 * {@code REF_invokeInterface} handle to
 * {@code Producer.abortTransaction:()V}; the outer method has no
 * {@code INVOKE*} for {@code abortTransaction}. To avoid false-positives the
 * rule treats any such captured handle as evidence the abort is wired. It does
 * NOT verify the executor actually fires it on exception — that is an
 * acknowledged false-negative kept to preserve HIGH confidence on the direct
 * call shape that this rule actually targets.
 *
 * <h2>Lambda-body false-negatives</h2>
 *
 * <p>A catch block of shape {@code catch (Exception e) { () -> producer.abortTransaction(); }}
 * compiles the abort call into a {@code lambda$N} synthetic method; the outer
 * method only contains an {@code INVOKEDYNAMIC} whose handle points at
 * {@code lambda$N}, not at {@code abortTransaction}. This rule does not descend
 * into synthetic methods to verify the lambda body, accepting that false-
 * negative — lambda-wrapped abort in a catch block is a rare shape, and
 * cross-method body inspection through arbitrary functional-interface lambdas
 * would widen the detection beyond what is needed for the bytecode signal
 * being targeted.
 */
public final class ProducerBeginTransactionNoAbortRule implements Rule {

    private static final String BEGIN_TRANSACTION = "beginTransaction";
    private static final String ABORT_TRANSACTION = "abortTransaction";
    private static final String VOID_NO_ARG_DESC = "()V";

    private final Severity severity;

    public ProducerBeginTransactionNoAbortRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_BEGIN_TRANSACTION_NO_ABORT;
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
        List<Integer> beginLines = new ArrayList<>();
        boolean hasAbort = false;
        int currentLine = -1;

        for (AbstractInsnNode insn : mn.instructions) {
            if (insn instanceof LineNumberNode ln) {
                currentLine = ln.line;
                continue;
            }
            if (insn instanceof MethodInsnNode mi && KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) {
                if (BEGIN_TRANSACTION.equals(mi.name) && VOID_NO_ARG_DESC.equals(mi.desc)) {
                    beginLines.add(currentLine);
                } else if (ABORT_TRANSACTION.equals(mi.name) && VOID_NO_ARG_DESC.equals(mi.desc)) {
                    hasAbort = true;
                }
                continue;
            }
            if (insn instanceof InvokeDynamicInsnNode indy
                    && AsmUtil.indyTargetHandle(indy, KafkaTypes.PRODUCER_OWNERS, ABORT_TRANSACTION, VOID_NO_ARG_DESC) != null) {
                hasAbort = true;
            }
        }

        if (beginLines.isEmpty() || hasAbort) return;

        String simpleClass = ctx.classNode().name.substring(ctx.classNode().name.lastIndexOf('/') + 1);
        for (int line : beginLines) {
            out.add(new Violation(
                    RuleId.PRODUCER_BEGIN_TRANSACTION_NO_ABORT, severity,
                    ctx.classNode().name, mn.name, line,
                    "Producer.beginTransaction() at " + simpleClass + "#" + mn.name
                            + " but Producer.abortTransaction() is not called anywhere in this method — if any of "
                            + "send/commit or any work between begin and commit throws, the transaction is left "
                            + "open. The producer's TransactionManager stays in IN_TRANSACTION (next "
                            + "beginTransaction() throws IllegalStateException: 'Invalid transition attempted from "
                            + "state IN_TRANSACTION to state IN_TRANSACTION'), and on the broker side every "
                            + "read_committed consumer of every partition the transaction wrote to is gated by the "
                            + "Last Stable Offset (LSO) until transaction.timeout.ms elapses — default 60s for "
                            + "plain producers, 10 min for Streams. Wrap the block: "
                            + "`try { producer.beginTransaction(); /* sends */ producer.commitTransaction(); } "
                            + "catch (Exception e) { producer.abortTransaction(); throw e; }`."));
        }
    }
}
