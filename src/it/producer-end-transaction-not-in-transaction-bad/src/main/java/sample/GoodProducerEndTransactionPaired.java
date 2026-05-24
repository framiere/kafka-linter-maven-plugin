package sample;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Control for PRODUCER_END_TRANSACTION_NOT_IN_TRANSACTION.
 *
 * <p>Six methods that each AVOID the rule by ensuring every
 * {@code commitTransaction()}/{@code abortTransaction()} call is preceded by
 * a matching unbalanced {@code beginTransaction()} on the same slot. Every
 * end-transaction call runs against a manager in {@code IN_TRANSACTION},
 * so the precondition check passes and no rule fires.
 *
 * <ol>
 *   <li>{@code beginCommit} — the canonical single-transaction shape:
 *       begin, send, commit.</li>
 *   <li>{@code beginAbort} — the canonical abort shape: begin, send,
 *       abort (used in error-recovery paths).</li>
 *   <li>{@code serialBeginCommitCycles} — two complete begin+commit
 *       cycles in one method. Each commit's matching begin is the most
 *       recent unbalanced one, so neither commit fires.</li>
 *   <li>{@code beginCommitBeginAbort} — first batch is committed, second
 *       is aborted (different terminal verdicts per batch). Both end-
 *       transaction calls have matching begins.</li>
 *   <li>{@code captureCommitAfterBegin} — method-reference
 *       {@code producer::commitTransaction} captured AFTER
 *       {@code beginTransaction()}. The slot is in beganSlots at the
 *       capture site, so the rule does not fire. (At runtime, the
 *       executor's worker is expected to run the captured commit while
 *       the transaction is still open.)</li>
 *   <li>{@code captureAbortAfterBegin} — method-reference
 *       {@code producer::abortTransaction} captured AFTER
 *       {@code beginTransaction()} for an error-recovery executor
 *       callback. Slot in beganSlots at capture; rule silent.</li>
 * </ol>
 */
public final class GoodProducerEndTransactionPaired {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties txnProducerProps(String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "good-end-txn-" + txnId);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    /** Correct: begin → send → commit (canonical single transaction). */
    public void beginCommit(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-begin-commit"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: begin → send → abort (canonical recovery shape). */
    public void beginAbort(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-begin-abort"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.abortTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: two complete begin+commit cycles serialised in one method. */
    public void serialBeginCommitCycles(ProducerRecord<String, String> first,
                                        ProducerRecord<String, String> second) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-serial-cycles"));
        try {
            producer.initTransactions();
            // Batch 1
            producer.beginTransaction();
            producer.send(first);
            producer.commitTransaction();
            // Batch 2
            producer.beginTransaction();
            producer.send(second);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: first batch committed, second batch aborted — each end has a matching begin. */
    public void beginCommitBeginAbort(ProducerRecord<String, String> first,
                                      ProducerRecord<String, String> second) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-commit-then-abort"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(first);
            producer.commitTransaction();
            producer.beginTransaction();
            producer.send(second);
            producer.abortTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: ::commitTransaction captured AFTER begin (slot in beganSlots at capture site). */
    public void captureCommitAfterBegin(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-capture-commit"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            executor.submit(producer::commitTransaction);
        } finally {
            executor.shutdownNow();
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: ::abortTransaction captured AFTER begin (slot in beganSlots at capture site). */
    public void captureAbortAfterBegin(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-capture-abort"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            executor.submit(producer::abortTransaction);
        } finally {
            executor.shutdownNow();
            producer.close(Duration.ofSeconds(10));
        }
    }
}
