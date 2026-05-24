package sample;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Silent — canonical EOS skeleton.
 *
 * <p>{@code beginTransaction()} is co-located with
 * {@code abortTransaction()} in the same method via a try/catch
 * that wraps the send + commit. The single-pass method-scope
 * scan finds the {@code abortTransaction()V} INVOKEINTERFACE
 * site, sets {@code hasAbort=true}, and the rule emits no
 * violation.
 *
 * <p>This is the shape the rule message recommends:
 * <pre>
 * try {
 *     producer.beginTransaction();
 *     // sends
 *     producer.commitTransaction();
 * } catch (Exception e) {
 *     producer.abortTransaction();
 *     throw e;
 * }
 * </pre>
 *
 * <p>The rule does NOT verify that the abort is in the catch
 * branch (vs the try branch) — it just verifies that an abort
 * call EXISTS somewhere in the same method. Code that intentionally
 * aborts in some non-catch branch (e.g. a guard on a precondition
 * that fails before any send) is still correct EOS shape and the
 * rule correctly stays silent.
 */
public final class GoodProducerWithCatchAbort {

    public void setup(Producer<String, String> producer) {
        producer.initTransactions();
    }

    public void processBatchWithAbort(Producer<String, String> producer,
                                       ProducerRecord<String, String> record) {
        try {
            producer.beginTransaction();
            producer.send(record);
            producer.commitTransaction();
        } catch (RuntimeException e) {
            producer.abortTransaction();
            throw e;
        }
    }
}
