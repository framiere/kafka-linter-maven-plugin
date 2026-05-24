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
 * Control for PRODUCER_SEND_OFFSETS_TO_TRANSACTION_NOT_IN_TRANSACTION.
 *
 * <p>Five methods that each AVOID the rule by ensuring every
 * {@code sendOffsetsToTransaction(...)} call is preceded by a matching
 * unbalanced {@code beginTransaction()} on the same slot. Every
 * sendOffsetsToTransaction call runs against a manager in
 * {@code IN_TRANSACTION}, so the precondition check passes and no rule
 * fires.
 *
 * <ol>
 *   <li>{@code beginSendOffsetsCommit} — the canonical EOS-v2
 *       poll-process-produce shape: begin, send, sendOffsets, commit.</li>
 *   <li>{@code beginSendOffsetsAbort} — error-recovery shape with abort
 *       instead of commit; sendOffsets still inside the begin/abort
 *       bracket, abort discards the offsets along with the records.</li>
 *   <li>{@code multipleSendOffsetsInOneTransaction} — three
 *       sendOffsetsToTransaction calls inside a single begin/commit
 *       bracket. sendOffsets is NON-terminal: the manager stays in
 *       IN_TRANSACTION after each call, so multiple sendOffsets within
 *       the same transaction is legitimate (per-source-topic offset
 *       aggregation, multi-batch offset bundling).</li>
 *   <li>{@code serialBeginSendOffsetsCommitCycles} — two complete
 *       begin+sendOffsets+commit cycles in one method. Each sendOffsets
 *       has a matching unbalanced begin above it.</li>
 *   <li>{@code beginSendOffsetsDeprecatedOverloadCommit} — the deprecated
 *       (Map, String) overload called INSIDE a transaction. The rule
 *       under test is silent (begin precedes the sendOffsets); the
 *       deprecated-overload-fencing rule
 *       PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED would normally
 *       fire on the String-groupId form, but is silenced in this IT's
 *       pom to isolate the state-machine rule under test.</li>
 * </ol>
 */
public final class GoodProducerSendOffsetsPaired {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties txnProducerProps(String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "good-send-offsets-" + txnId);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    private static Map<TopicPartition, OffsetAndMetadata> offsetsFor(TopicPartition tp, long nextOffset) {
        return Collections.singletonMap(tp, new OffsetAndMetadata(nextOffset));
    }

    /** Correct: canonical EOS-v2 poll-process-produce. */
    public void beginSendOffsetsCommit(ProducerRecord<String, String> record,
                                       TopicPartition tp, long nextOffset) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-send-offsets-commit"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.sendOffsetsToTransaction(offsetsFor(tp, nextOffset), new ConsumerGroupMetadata("consumer-group"));
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: sendOffsets inside a begin/abort bracket — abort discards offsets along with records. */
    public void beginSendOffsetsAbort(ProducerRecord<String, String> record,
                                      TopicPartition tp, long nextOffset) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-send-offsets-abort"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.sendOffsetsToTransaction(offsetsFor(tp, nextOffset), new ConsumerGroupMetadata("consumer-group"));
            producer.abortTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: three sendOffsets calls inside one begin/commit bracket (non-terminal — manager stays IN_TRANSACTION). */
    public void multipleSendOffsetsInOneTransaction(ProducerRecord<String, String> record,
                                                    TopicPartition tp1, long nextOffset1,
                                                    TopicPartition tp2, long nextOffset2,
                                                    TopicPartition tp3, long nextOffset3) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-multi-send-offsets"));
        try {
            producer.initTransactions();
            ConsumerGroupMetadata groupMd = new ConsumerGroupMetadata("consumer-group");
            producer.beginTransaction();
            producer.send(record);
            producer.sendOffsetsToTransaction(offsetsFor(tp1, nextOffset1), groupMd);
            producer.sendOffsetsToTransaction(offsetsFor(tp2, nextOffset2), groupMd);
            producer.sendOffsetsToTransaction(offsetsFor(tp3, nextOffset3), groupMd);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: two complete begin+sendOffsets+commit cycles serialised in one method. */
    public void serialBeginSendOffsetsCommitCycles(ProducerRecord<String, String> first,
                                                   ProducerRecord<String, String> second,
                                                   TopicPartition tp, long nextOffset1, long nextOffset2) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-serial-send-offsets"));
        try {
            producer.initTransactions();
            ConsumerGroupMetadata groupMd = new ConsumerGroupMetadata("consumer-group");
            // Batch 1
            producer.beginTransaction();
            producer.send(first);
            producer.sendOffsetsToTransaction(offsetsFor(tp, nextOffset1), groupMd);
            producer.commitTransaction();
            // Batch 2
            producer.beginTransaction();
            producer.send(second);
            producer.sendOffsetsToTransaction(offsetsFor(tp, nextOffset2), groupMd);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Correct: deprecated (Map, String) overload INSIDE a transaction.
     * The state-machine rule under test is silent because the begin
     * precedes the sendOffsets. The deprecated-overload-fencing rule
     * PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED is silenced via the
     * IT's pom to isolate the rule under test.
     */
    @SuppressWarnings("deprecation")
    public void beginSendOffsetsDeprecatedOverloadCommit(ProducerRecord<String, String> record,
                                                         TopicPartition tp, long nextOffset) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-send-offsets-deprecated"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.sendOffsetsToTransaction(offsetsFor(tp, nextOffset), "consumer-group");
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }
}
