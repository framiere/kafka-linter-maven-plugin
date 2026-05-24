package sample;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Silent — abort wired via captured method-reference.
 *
 * <p>The catch path can wire the abort via a captured
 * method-reference, e.g.
 * {@code attempt.recover(producer::abortTransaction)} or
 * {@code executor.execute(producer::abortTransaction)}. javac
 * compiles {@code producer::abortTransaction} into an
 * INVOKEDYNAMIC at the call site whose bootstrap-method args
 * include a {@code REF_invokeVirtual} /
 * {@code REF_invokeInterface} handle pointing at
 * {@code Producer.abortTransaction:()V}. The outer method
 * bytecode therefore contains zero {@code INVOKE*} instructions
 * directly targeting {@code abortTransaction} — a
 * MethodInsnNode-only scan would (incorrectly) conclude
 * {@code hasAbort=false} and false-positive.
 *
 * <p>The rule's INVOKEDYNAMIC walk inspects bsmArgs handles via
 * {@code AsmUtil.indyTargetHandle(indy, PRODUCER_OWNERS,
 * "abortTransaction", "()V")} and treats any captured handle as
 * evidence that abort is wired. {@code hasAbort=true} is set and
 * the rule emits no violation.
 *
 * <p>The rule deliberately does NOT verify that the executor /
 * recovery wrapper actually fires the handle on exception — that
 * acknowledged false-negative is the price of recognising this
 * shape at all without an inter-procedural analysis. Method-ref
 * wiring of abort is a common shape in functional-style EOS
 * helpers, and false-positives on it would render the rule
 * unusable in those codebases.
 */
public final class GoodProducerWithMethodRefAbort {

    public void setup(Producer<String, String> producer) {
        producer.initTransactions();
    }

    public void processBatchWithMethodRefAbort(Producer<String, String> producer,
                                                ProducerRecord<String, String> record,
                                                FailureExecutor onFailure) {
        producer.beginTransaction();
        onFailure.runOnException(producer::abortTransaction);
        producer.send(record);
        producer.commitTransaction();
    }

    /** Local FunctionalInterface so the fixture is self-contained. */
    public interface FailureExecutor {
        void runOnException(Runnable r);
    }
}
