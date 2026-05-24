package sample;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Control for PRODUCER_BEGIN_TRANSACTION_TWICE.
 *
 * <p>Three methods that each avoid the rule by following one of the only
 * correct shapes: open a transaction with exactly one
 * {@code beginTransaction()} and close it with exactly one matching
 * {@code commitTransaction()} or {@code abortTransaction()} on every code
 * path that left {@code READY}. To run a SECOND transaction in the same
 * method, drive the manager back to {@code READY} first.
 *
 * <ol>
 *   <li>{@code singleTransaction} — the canonical EOS skeleton: init,
 *       begin, send, commit, close. One begin, one commit, no second
 *       begin anywhere.</li>
 *   <li>{@code serialTransactionsWithCommitBetween} — two
 *       beginTransaction() calls are LEGAL when separated by a
 *       commitTransaction() that drives the manager from
 *       {@code COMMITTING_TRANSACTION} back to {@code READY} — the
 *       second begin is the start of a new, independent transaction.
 *       Rule honors commit() as a reset and does NOT fire.</li>
 *   <li>{@code retryWithAbortBetween} — the documented recovery shape:
 *       a failed first transaction is aborted (driving the manager
 *       from {@code ABORTING_TRANSACTION} back to {@code READY}), then
 *       a fresh begin() opens a new transaction. Rule honors abort()
 *       as a reset and does NOT fire.</li>
 * </ol>
 */
public final class GoodProducerSingleTransaction {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties txnProducerProps(String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "good-single-txn-" + txnId);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    /** Correct: single transaction lifecycle — one begin, one commit. */
    public void singleTransaction(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-single"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.commitTransaction();
        } catch (RuntimeException e) {
            producer.abortTransaction();
            throw e;
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: two serial transactions separated by commitTransaction() — manager returns to READY between them. */
    public void serialTransactionsWithCommitBetween(ProducerRecord<String, String> first,
                                                    ProducerRecord<String, String> second) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-serial-commits"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(first);
            // commitTransaction() drives the manager back to READY; the next
            // beginTransaction() starts a NEW, independent transaction.
            producer.commitTransaction();
            producer.beginTransaction();
            producer.send(second);
            producer.commitTransaction();
        } catch (RuntimeException e) {
            producer.abortTransaction();
            throw e;
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: retry shape — abort drives manager back to READY before the re-begin. */
    public void retryWithAbortBetween(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-retry-with-abort"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            try {
                producer.send(record);
                producer.commitTransaction();
            } catch (RuntimeException e) {
                // Documented recovery: abort first (drives manager from
                // ABORTING_TRANSACTION back to READY), THEN re-begin. The rule
                // honors abort() as the reset call and does NOT fire on the
                // subsequent begin.
                producer.abortTransaction();
                producer.beginTransaction();
                producer.send(record);
                producer.commitTransaction();
            }
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }
}
