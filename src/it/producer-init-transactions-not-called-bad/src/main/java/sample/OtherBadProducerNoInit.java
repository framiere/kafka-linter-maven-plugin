package sample;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Second class exercising PRODUCER_INIT_TRANSACTIONS_NOT_CALLED.
 *
 * <p>Project-scoped rules look at EVERY {@code .class} file in
 * {@code target/classes}. This class confirms that the rule does
 * not stop at the first class it sees — txn-lifecycle sites in
 * this class are reported just as sites in
 * {@link BadProducerNoInitTransactions} are. As long as no class
 * anywhere in the project calls {@code initTransactions()}, every
 * lifecycle call across every class becomes a violation.
 *
 * <p>This class is deliberately minimal: one method, one lifecycle
 * call (commitTransaction), so it adds exactly one expected fire
 * to the total.
 */
public final class OtherBadProducerNoInit {

    public void emitAndCommit(Producer<String, String> producer,
                              ProducerRecord<String, String> record) {
        producer.send(record);
        producer.commitTransaction(); // reported — different class, same missing-init project state
    }
}
