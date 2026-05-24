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
 * RULE: PRODUCER_END_TRANSACTION_NOT_IN_TRANSACTION.
 *
 * <p>Fires when {@code Producer.commitTransaction()} or
 * {@code Producer.abortTransaction()} is called on the same local slot in the
 * same method without a matching unbalanced {@code beginTransaction()}
 * preceding it. The producer's internal {@code TransactionManager} only
 * allows the {@code IN_TRANSACTION → READY} edge via the commit/abort
 * handshake; an end-transaction call against any other state (READY,
 * UNINITIALIZED, INITIALIZING, COMMITTING_TRANSACTION, ABORTING_TRANSACTION,
 * FATAL_ERROR) throws {@code KafkaException("Invalid transition attempted
 * from state READY to state COMMITTING_TRANSACTION")} (or {@code state
 * ABORTING_TRANSACTION}) before any RPC and no transaction marker reaches
 * the broker.
 *
 * <p>The bug shape is structurally common because developers reach for
 * 'safety net' wrappers around commit/abort calls without ensuring a
 * matching {@code beginTransaction()} ran above. The surrounding
 * try/catch hides the failure and the application proceeds in a degraded
 * state where it believes its records were transactionally bracketed when
 * in reality they reached the broker as non-transactional sends
 * (idempotent producer not in a transaction is legal — and silently
 * defeats the {@code read_committed} guarantee).
 *
 * <p>What this rule catches (four direct-call shapes + one method-reference
 * capture, each firing exactly once):
 * <ol>
 *   <li>{@code commitWithoutBegin} — no {@code beginTransaction()} anywhere
 *       above the {@code commitTransaction()} call. The commit runs
 *       against a manager in {@code READY} and throws KafkaException.</li>
 *   <li>{@code abortAfterCommitCycle} — a clean
 *       {@code begin/send/commit} cycle is followed by an
 *       {@code abortTransaction()} call (often added 'just in case' or as
 *       a copy-paste from a different code path). The commit drove the
 *       manager back to {@code READY}; the abort throws.</li>
 *   <li>{@code doubleCommitSafetyNet} — a single
 *       {@code beginTransaction()} is paired with TWO consecutive
 *       {@code commitTransaction()} calls (the second is a 'safety net').
 *       The first commit drives the manager from {@code IN_TRANSACTION}
 *       back to {@code READY}; the second commit throws.</li>
 *   <li>{@code doubleAbortSafetyNet} — a single {@code beginTransaction()}
 *       is paired with TWO consecutive {@code abortTransaction()} calls
 *       (the second is a 'safety net for an error-recovery path'). The
 *       first abort drives the manager back to {@code READY}; the second
 *       abort throws.</li>
 *   <li>{@code captureCommitWithoutBegin} — method-reference
 *       {@code producer::commitTransaction} captured for an executor
 *       callback without any preceding {@code beginTransaction()} in the
 *       same method. The capturing INVOKEDYNAMIC bsmArgs hold a
 *       {@code REF_invokeVirtual} handle to
 *       {@code KafkaProducer.commitTransaction*}; when the executor runs
 *       the captured Runnable, the manager is in {@code READY}, commit
 *       throws KafkaException, the executor's default uncaught-handler
 *       swallows it.</li>
 * </ol>
 *
 * <p>Correct pattern: every {@code commitTransaction()}/{@code
 * abortTransaction()} call must be preceded by exactly one matching
 * {@code beginTransaction()} on the same slot, with no unmatched
 * intervening end-transaction. The {@code GoodProducerEndTransactionPaired}
 * silent controls cover the canonical correct shapes.
 */
public final class BadProducerEndTransactionNotInTransaction {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties txnProducerProps(String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "end-not-in-txn-" + txnId);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    /**
     * Anti-pattern #1: commitTransaction() called without any preceding
     * beginTransaction() in the method. Typical origin: the method sends
     * a record and 'commits' to mark the work as done, but the
     * transactional bracket was never opened — perhaps a copy-paste from
     * a non-transactional path that kept the commit but lost the begin.
     * At runtime the commit runs on a manager in READY and throws
     * KafkaException; the surrounding try/catch hides the failure.
     */
    public void commitWithoutBegin(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-commit-no-begin"));
        try {
            producer.initTransactions();
            producer.send(record);
            producer.commitTransaction(); // FIRES — manager is READY, no preceding beginTransaction
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #2: clean begin/send/commit cycle followed by an
     * abortTransaction() call (often added 'just in case' or via a
     * copy-paste from a different code path). The commit drove the manager
     * back to READY; the abort runs against READY and throws
     * KafkaException("Invalid transition attempted from state READY to
     * state ABORTING_TRANSACTION").
     */
    public void abortAfterCommitCycle(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-abort-after-commit"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.commitTransaction();
            producer.abortTransaction(); // FIRES — commit already drove manager back to READY
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #3: a single beginTransaction() paired with TWO
     * consecutive commitTransaction() calls (the second is a 'safety net
     * in case the first didn't take effect'). The first commit drives
     * the manager from IN_TRANSACTION back to READY; the second commit
     * runs against READY and throws KafkaException. Both calls appear
     * successful in code, but only the first one actually wrote a commit
     * marker to the broker.
     */
    public void doubleCommitSafetyNet(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-double-commit"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.commitTransaction();
            producer.commitTransaction(); // FIRES — manager already returned to READY
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #4: a single beginTransaction() paired with TWO
     * consecutive abortTransaction() calls (the second is a 'safety net'
     * for an error-recovery path). The first abort drives the manager
     * from IN_TRANSACTION back to READY; the second abort runs against
     * READY and throws KafkaException. Only the first abort wrote an
     * abort marker; the second is a silent no-op.
     */
    public void doubleAbortSafetyNet(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-double-abort"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.abortTransaction();
            producer.abortTransaction(); // FIRES — manager already returned to READY
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #5: producer::commitTransaction method-reference
     * captured WITHOUT any preceding beginTransaction() in the same
     * method. The capturing INVOKEDYNAMIC bsmArgs hold a
     * REF_invokeVirtual handle to KafkaProducer.commitTransaction*; when
     * the executor's worker thread runs the captured Runnable, the
     * manager is in READY (no begin happened), the commit throws
     * KafkaException, the executor's default uncaught-handler swallows
     * it and the 'scheduled commit' is a silent no-op for the lifetime
     * of the process.
     */
    public void captureCommitWithoutBegin(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-capture-commit"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            producer.initTransactions();
            producer.send(record);
            executor.submit(producer::commitTransaction); // FIRES — ::commitTransaction captured with no preceding begin
        } finally {
            executor.shutdownNow();
            producer.close(Duration.ofSeconds(10));
        }
    }
}
