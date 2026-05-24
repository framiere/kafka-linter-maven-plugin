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
 * RULE: PRODUCER_BEGIN_TRANSACTION_TWICE.
 *
 * <p>Fires when {@code Producer.beginTransaction()} is invoked twice on the
 * same local slot in the same method without an intervening
 * {@code commitTransaction()} / {@code abortTransaction()}. The producer's
 * internal {@code TransactionManager} only allows the
 * {@code READY → IN_TRANSACTION} edge via {@code beginTransaction()}; the
 * second call executes against a manager already in {@code IN_TRANSACTION}
 * and throws {@code KafkaException("Invalid transition attempted from state
 * IN_TRANSACTION to state IN_TRANSACTION")} before any RPC.
 *
 * <p>The bug shape is structurally common in code that grew incrementally:
 * a transactional helper was refactored to call {@code beginTransaction()}
 * itself without removing the outer caller's {@code beginTransaction()}, OR
 * a retry handler tries to 'restart' a failed transaction by calling
 * {@code beginTransaction()} again without first calling
 * {@code abortTransaction()} to drive the state machine back to
 * {@code READY}.
 *
 * <p>What this rule catches (three method-scope shapes):
 * <ol>
 *   <li>{@code outerThenInnerBegin} — outer method calls
 *       {@code beginTransaction()} to wrap a batch, then calls
 *       {@code beginTransaction()} a second time before any commit/abort.
 *       The second begin throws KafkaException; the FIRST transaction
 *       remains open and blocks {@code read_committed} consumers
 *       downstream for the full {@code transaction.timeout.ms} window.</li>
 *   <li>{@code serialBatchWithDuplicateBeginInSecond} — first transaction
 *       is opened and committed cleanly (legitimate); a second transaction
 *       is then opened, but the second batch accidentally has TWO begins
 *       before its commit. The commit between the two batches drives the
 *       manager back to {@code READY} (so the second begin is fine), but
 *       the THIRD begin inside the second batch's window throws — the rule
 *       reports it at the second-begin-of-the-second-batch site.</li>
 *   <li>{@code beginThenCaptureBeginReference} — first
 *       {@code beginTransaction()} puts the manager into
 *       {@code IN_TRANSACTION}; a method-reference
 *       {@code producer::beginTransaction} captured for a 'rescue' executor
 *       callback fires KafkaException on the executor's worker thread when
 *       invoked; the executor's uncaught-exception handler swallows it.</li>
 * </ol>
 *
 * <p>Correct pattern: a transaction is opened by exactly one
 * {@code beginTransaction()} call and closed by exactly one matching
 * {@code commitTransaction()} or {@code abortTransaction()} on every code
 * path that left {@code READY}. To run a SECOND transaction in the same
 * method, drive the manager back to {@code READY} first via commit or
 * abort. The {@code GoodProducerSingleTransaction} silent controls cover
 * the legitimate shapes.
 */
public final class BadProducerBeginTransactionTwice {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties txnProducerProps(String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "begin-twice-" + txnId);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    /**
     * Anti-pattern #1: outer wraps a batch in beginTransaction(), then
     * calls beginTransaction() a SECOND time before any commit/abort —
     * typically because a helper method was inlined that itself opened
     * a transaction, and the outer caller's begin was never removed.
     */
    public void outerThenInnerBegin(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-outer-then-inner"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            // Helper that 'wraps' a single record in its own transaction was
            // inlined here; the outer begin() was never removed.
            producer.beginTransaction(); // FIRES — manager is already IN_TRANSACTION
            producer.send(record);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #2: two serial transactions in one method — the first
     * is clean (begin, send, commit), but the second accidentally
     * contains a duplicate beginTransaction() before its commit. The
     * commit between the two batches drives the manager back to
     * {@code READY} cleanly, the second begin is legitimate, but the
     * THIRD begin throws because the manager is in
     * {@code IN_TRANSACTION} again from the second begin. Models a
     * common shape: 'process batch 1; process batch 2' where the second
     * batch's helper inlined a begin that the outer caller's begin had
     * already done.
     */
    public void serialBatchWithDuplicateBeginInSecond(ProducerRecord<String, String> first,
                                                      ProducerRecord<String, String> second) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-serial-dup-begin"));
        try {
            producer.initTransactions();
            // Batch 1: clean lifecycle.
            producer.beginTransaction();
            producer.send(first);
            producer.commitTransaction();
            // Batch 2: outer begin + accidental duplicate begin from an
            // inlined helper that was never reconciled with the outer
            // caller's begin.
            producer.beginTransaction();
            producer.beginTransaction(); // FIRES — manager is already IN_TRANSACTION from previous line
            producer.send(second);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #3: producer::beginTransaction method-reference
     * captured AFTER an in-line beginTransaction(). The capturing
     * INVOKEDYNAMIC bsmArgs hold a REF_invokeVirtual handle to
     * KafkaProducer.beginTransaction()V — when the executor's worker
     * thread runs the captured ref, the manager is already
     * IN_TRANSACTION and the second begin throws KafkaException; the
     * executor's uncaught-exception handler typically swallows it and
     * the 'rescue' silently fails.
     */
    public void beginThenCaptureBeginReference(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-begin-capture"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            // 'Rescue handler' that re-opens the transaction during a perceived
            // coordinator hiccup — the captured method reference is invoked on
            // the executor's worker. The manager is still IN_TRANSACTION, so
            // beginTransaction() throws KafkaException; the executor's
            // uncaught-handler swallows it and the rescue silently fails.
            Runnable rescueBegin = producer::beginTransaction; // FIRES — ::beginTransaction captured after begin
            executor.submit(rescueBegin);
            producer.commitTransaction();
        } finally {
            executor.shutdownNow();
            producer.close(Duration.ofSeconds(10));
        }
    }
}
