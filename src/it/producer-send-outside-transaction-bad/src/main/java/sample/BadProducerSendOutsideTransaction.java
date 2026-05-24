package sample;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * RULE: PRODUCER_SEND_OUTSIDE_TRANSACTION.
 *
 * <p>Fires when {@code Producer.send(...)} is called on a TRANSACTIONAL
 * KafkaProducer slot (one that has had {@code initTransactions()} called on it
 * earlier in the same method) when no {@code beginTransaction()} is currently
 * open — either before any begin, after a commit/abort with no new begin, or
 * between an abort and a re-begin. The producer's
 * {@code TransactionManager.maybeAddPartitionToTransaction()} check sees
 * {@code currentState != IN_TRANSACTION} (it is READY, or post-FATAL_ERROR)
 * and throws
 * {@code IllegalStateException("Cannot add partition <tp> to transaction
 * since the transaction is not in progress, set currentState= READY.")}
 * synchronously, BEFORE the record reaches the accumulator and BEFORE any RPC.
 *
 * <p>The bug shape is structurally common: developers write 'happy-path'
 * transactional loops and accidentally move the send() outside the
 * begin/commit bracket during a refactor, OR add a 'safety send' (audit,
 * footer, retry-audit) outside the bracket. The runtime exception type is
 * {@code IllegalStateException}, NOT {@code KafkaException} — distinct from
 * the rest of the transactional-error family — so error-handling code that
 * catches {@code KafkaException} specifically silently swallows the exception
 * under a different code path. The Callback-overload variant is doubly
 * invisible because the callback is never invoked (the exception fires BEFORE
 * the record is enqueued, so the IO thread never sees the record).
 *
 * <p>What this rule catches (five direct-call shapes, each firing exactly
 * once or twice at the orphan send site):
 * <ol>
 *   <li>{@code sendBeforeBegin} — init then send then begin then send then
 *       commit. The first send is orphan (state is READY between init and
 *       begin). FIRES once.</li>
 *   <li>{@code sendAfterCommit} — init then begin then send then commit then
 *       send. The post-commit send is orphan (state went back to READY after
 *       commit). FIRES once.</li>
 *   <li>{@code sendAfterAbort} — init then begin then send then abort then
 *       send. The post-abort send is orphan (state went back to READY after
 *       abort). FIRES once.</li>
 *   <li>{@code sendWithNoBeginEver} — init then send (no begin at all in this
 *       method). FIRES once.</li>
 *   <li>{@code sendBetweenAbortAndReBegin} — init then begin then send then
 *       abort then send (orphan) then begin then send then commit. The
 *       orphan-send in the middle FIRES once.</li>
 * </ol>
 *
 * <p>Correct pattern: every send() on a transactional producer must sit
 * BETWEEN a beginTransaction() and a matching commitTransaction()/
 * abortTransaction() in the same execution path. If non-transactional sends
 * (audits, footers, fire-and-forget) are needed, use a SEPARATE producer
 * without a transactional.id.
 */
public final class BadProducerSendOutsideTransaction {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties txnProducerProps(String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "send-outside-txn-" + txnId);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    /**
     * Anti-pattern #1: send BEFORE begin. A refactor moved the
     * beginTransaction() below the first send() and the developer did not
     * notice. The first send runs against a producer in READY state (init
     * succeeded but no begin yet), maybeAddPartition throws
     * IllegalStateException, the record is dropped. The subsequent begin/
     * send/commit cycle succeeds, so for every batch the FIRST record is
     * silently lost and the rest are committed transactionally.
     */
    public void sendBeforeBegin(ProducerRecord<String, String> first, ProducerRecord<String, String> second) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-send-before-begin"));
        try {
            producer.initTransactions();
            producer.send(first); // FIRES — no begin yet, state is READY
            producer.beginTransaction();
            producer.send(second);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #2: send AFTER commit. The producer has been correctly
     * bracketed (init/begin/send/commit) but a 'footer' send was added after
     * the commit 'to mark the end of the batch'. The footer's send runs on a
     * producer in READY (commit drove it back to READY); throws
     * IllegalStateException; the footer is dropped and downstream tooling
     * never sees a batch-boundary marker.
     */
    public void sendAfterCommit(ProducerRecord<String, String> record, ProducerRecord<String, String> footer) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-send-after-commit"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.commitTransaction();
            producer.send(footer); // FIRES — state is READY after commit
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #3: send AFTER abort. An error-handler aborts the
     * transaction and then tries to send a 'retry-audit' record outside the
     * transaction to record that the abort happened. The audit send runs on a
     * producer in READY (abort drove it back to READY); throws
     * IllegalStateException; the audit subsystem records ZERO records and
     * every abort is silently un-audited.
     */
    public void sendAfterAbort(ProducerRecord<String, String> record, ProducerRecord<String, String> audit) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-send-after-abort"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.abortTransaction();
            producer.send(audit); // FIRES — state is READY after abort
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #4: send with NO begin EVER. The producer is configured
     * transactionally (initTransactions() was called) but the developer
     * forgot to add the begin/commit bracket entirely. Every send runs on a
     * producer in READY; every send throws IllegalStateException; every
     * record is dropped. The application logs 'send failed' on every record
     * and the operator concludes 'the broker is unreachable'.
     */
    public void sendWithNoBeginEver(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-send-no-begin"));
        try {
            producer.initTransactions();
            producer.send(record); // FIRES — no begin at all in this method
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #5: send BETWEEN abort and re-begin. A retry-loop aborts
     * the failed transaction and tries to record a 'retry-audit' BEFORE
     * starting the retry's begin. The audit send runs between abort (state
     * back to READY) and the next begin (state still READY); throws
     * IllegalStateException; the audit is dropped. Surrounding error-handling
     * logs 'audit send failed' and the operator chases a phantom audit-system
     * bug.
     */
    public void sendBetweenAbortAndReBegin(ProducerRecord<String, String> record, ProducerRecord<String, String> audit, ProducerRecord<String, String> retry) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-send-between-abort-and-rebegin"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.abortTransaction();
            producer.send(audit); // FIRES — orphan send between abort and re-begin
            producer.beginTransaction();
            producer.send(retry);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }
}
