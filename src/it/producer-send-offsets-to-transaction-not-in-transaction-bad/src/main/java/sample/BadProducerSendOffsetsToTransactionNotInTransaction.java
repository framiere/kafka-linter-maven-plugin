package sample;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * RULE: PRODUCER_SEND_OFFSETS_TO_TRANSACTION_NOT_IN_TRANSACTION.
 *
 * <p>Fires when {@code Producer.sendOffsetsToTransaction(...)} is called on the
 * same local slot in the same method WITHOUT a matching unbalanced
 * {@code beginTransaction()} preceding it. The producer's internal
 * {@code TransactionManager} only allows the call while currentState is
 * {@code IN_TRANSACTION}; any other state (READY, UNINITIALIZED, INITIALIZING,
 * COMMITTING_TRANSACTION, ABORTING_TRANSACTION, FATAL_ERROR) throws
 * {@code KafkaException("Cannot send offsets if a transaction is not in
 * progress (currentState= <state>).")} before any RPC and the offsets never
 * reach __consumer_offsets — the consumer's last-committed offset stays
 * frozen at its previous value and every restart/rebalance replays every
 * record that the lost-then-found 'transactional commit' was meant to ack.
 *
 * <p>The bug shape is structurally common in poll-process-produce loops:
 * developers reach for offset-commit-with-transaction patterns but accidentally
 * place the call OUTSIDE the begin/commit bracket — either before begin,
 * after commit, or in a 'safety net' branch where the transaction never
 * opened. The surrounding try/catch swallows the exception and the application
 * proceeds in a degraded state where it believes the offsets were committed
 * transactionally but in reality they weren't committed at all.
 *
 * <p>What this rule catches (five direct-call shapes, each firing exactly
 * once):
 * <ol>
 *   <li>{@code sendOffsetsBeforeBegin} — sendOffsetsToTransaction called
 *       BEFORE any beginTransaction in the method. The manager is in READY,
 *       the call throws KafkaException.</li>
 *   <li>{@code sendOffsetsAfterCommitCycle} — a begin+sendOffsets+commit
 *       cycle is followed by ANOTHER sendOffsetsToTransaction call. The
 *       commit drove the manager back to READY; the trailing sendOffsets
 *       runs against READY and throws.</li>
 *   <li>{@code sendOffsetsAfterAbortCycle} — a begin+abort cycle is
 *       followed by a sendOffsetsToTransaction call. The abort drove the
 *       manager back to READY; the trailing sendOffsets throws.</li>
 *   <li>{@code sendOffsetsNoTransactionAtAll} — no beginTransaction
 *       anywhere in the method. The sendOffsets runs against an
 *       UNINITIALIZED/READY manager and throws.</li>
 *   <li>{@code sendOffsetsDeprecatedOverloadNoBegin} — the deprecated
 *       {@code sendOffsetsToTransaction(Map, String groupId)} overload
 *       called without a preceding begin. Proves the rule matches by name
 *       only (covers both the modern KIP-447 overload and the deprecated
 *       String-groupId overload — both share the IN_TRANSACTION
 *       precondition).</li>
 * </ol>
 *
 * <p>Correct pattern: every {@code sendOffsetsToTransaction(...)} call must
 * sit BETWEEN a matching {@code beginTransaction()} and the corresponding
 * {@code commitTransaction()}/{@code abortTransaction()} on the same slot.
 * Multiple sendOffsets calls within the same transaction are legitimate
 * (the call is NON-terminal — the manager stays in IN_TRANSACTION). The
 * {@code GoodProducerSendOffsetsPaired} silent controls cover the canonical
 * correct shapes.
 */
public final class BadProducerSendOffsetsToTransactionNotInTransaction {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties txnProducerProps(String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "send-offsets-not-in-txn-" + txnId);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    private static Map<TopicPartition, OffsetAndMetadata> offsetsFor(TopicPartition tp, long nextOffset) {
        return Collections.singletonMap(tp, new OffsetAndMetadata(nextOffset));
    }

    /**
     * Anti-pattern #1: sendOffsetsToTransaction() called BEFORE any
     * beginTransaction() in the method. Typical origin: a developer
     * copy-pasted from a non-transactional offset-commit path that used
     * {@code consumer.commitSync(offsets)} and 'upgraded' to the
     * transactional variant without moving the call inside the begin/commit
     * bracket. At runtime the call runs against a manager in READY and
     * throws KafkaException.
     */
    public void sendOffsetsBeforeBegin(ProducerRecord<String, String> record,
                                       TopicPartition tp, long nextOffset) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-send-offsets-before-begin"));
        try {
            producer.initTransactions();
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsFor(tp, nextOffset);
            producer.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata("consumer-group")); // FIRES — no begin yet
            producer.beginTransaction();
            producer.send(record);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #2: a complete begin+sendOffsets+commit cycle is
     * followed by ANOTHER sendOffsetsToTransaction() call (often added
     * 'just in case' or as a copy-paste from a different code path).
     * The commit drove the manager back to READY; the trailing sendOffsets
     * runs against READY and throws KafkaException.
     */
    public void sendOffsetsAfterCommitCycle(ProducerRecord<String, String> record,
                                            TopicPartition tp, long nextOffset) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-send-offsets-after-commit"));
        try {
            producer.initTransactions();
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsFor(tp, nextOffset);
            ConsumerGroupMetadata groupMd = new ConsumerGroupMetadata("consumer-group");
            producer.beginTransaction();
            producer.send(record);
            producer.sendOffsetsToTransaction(offsets, groupMd);
            producer.commitTransaction();
            producer.sendOffsetsToTransaction(offsets, groupMd); // FIRES — commit drove manager back to READY
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #3: a begin+abort cycle is followed by a
     * sendOffsetsToTransaction() call (often added in error-recovery code
     * that 'preserves consumer progress' after an abort — but the only way
     * to commit offsets transactionally is INSIDE the next transaction's
     * begin/end bracket). The abort drove the manager back to READY; the
     * trailing sendOffsets runs against READY and throws KafkaException.
     */
    public void sendOffsetsAfterAbortCycle(ProducerRecord<String, String> record,
                                           TopicPartition tp, long nextOffset) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-send-offsets-after-abort"));
        try {
            producer.initTransactions();
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsFor(tp, nextOffset);
            ConsumerGroupMetadata groupMd = new ConsumerGroupMetadata("consumer-group");
            producer.beginTransaction();
            producer.send(record);
            producer.abortTransaction();
            producer.sendOffsetsToTransaction(offsets, groupMd); // FIRES — abort drove manager back to READY
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #4: NO beginTransaction() anywhere in the method.
     * Typical origin: a developer wired up a 'fast path' for empty-batch
     * heartbeat polls — 'if the batch is empty, skip the begin/end bracket
     * but still commit offsets to advance position'. At runtime the
     * sendOffsets call runs against an UNINITIALIZED or READY manager and
     * throws KafkaException. The fast-path's catch block swallows the
     * exception and the operator never learns that empty-batch heartbeat
     * offsets are silently dropped.
     */
    public void sendOffsetsNoTransactionAtAll(TopicPartition tp, long nextOffset) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-send-offsets-no-txn"));
        try {
            producer.initTransactions();
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsFor(tp, nextOffset);
            producer.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata("consumer-group")); // FIRES — no begin in method
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #5: the DEPRECATED (Map, String groupId) overload called
     * without a preceding beginTransaction(). Proves the rule matches by
     * NAME only and catches both the modern (Map, ConsumerGroupMetadata)
     * overload (KIP-447, Kafka 2.5+) and the deprecated String-groupId
     * overload — both share the IN_TRANSACTION precondition check.
     * (The deprecated-overload-fencing rule
     * PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED is silenced in this
     * IT's pom to isolate the rule under test.)
     */
    @SuppressWarnings("deprecation")
    public void sendOffsetsDeprecatedOverloadNoBegin(TopicPartition tp, long nextOffset) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-send-offsets-deprecated-no-begin"));
        try {
            producer.initTransactions();
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsFor(tp, nextOffset);
            producer.sendOffsetsToTransaction(offsets, "consumer-group"); // FIRES — deprecated overload, no begin
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }
}
