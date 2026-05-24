package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;

/**
 * Control for CONSUMER_COMMIT_OFFSET_OFF_BY_ONE.
 *
 * <p>Seven methods that each AVOID the rule by following one of the
 * canonical correct shapes. The rule's detection is structural: it fires
 * only on {@code new OffsetAndMetadata(record.offset())} where the
 * IMMEDIATELY preceding significant instruction is
 * {@code INVOKEVIRTUAL ConsumerRecord.offset()J} with NO intervening
 * arithmetic. Any of the following shapes interpose a different
 * instruction before the {@code <init>} (or do not feed the {@code offset()}
 * call directly into the constructor) and are therefore silent:
 *
 * <ol>
 *   <li>{@code commitSyncPlusOneInline} — the canonical correct form
 *       {@code new OffsetAndMetadata(record.offset() + 1)}. Compiles to
 *       {@code INVOKEVIRTUAL offset()J + LCONST_1 + LADD} between the
 *       {@code offset()} call and the {@code <init>}, so the rule's
 *       {@code prevSignificant} check fails (it sees {@code LADD}, not
 *       {@code INVOKEVIRTUAL}).</li>
 *   <li>{@code commitSyncPlusOneMetadata} — same {@code + 1} with the
 *       two-arg metadata constructor.</li>
 *   <li>{@code commitSyncPlusOneLeaderEpoch} — same {@code + 1} with the
 *       three-arg leader-epoch constructor.</li>
 *   <li>{@code commitAsyncPlusOne} — same {@code + 1} for the async path.</li>
 *   <li>{@code sendOffsetsToTransactionPlusOne} — same {@code + 1} for the
 *       EOS-v2 transactional commit path.</li>
 *   <li>{@code accumulatedOffsetsPlusOne} — batch commit that walks the
 *       {@code ConsumerRecords} and stores {@code record.offset() + 1} per
 *       partition. The natural correct shape for end-of-poll bulk commits.</li>
 *   <li>{@code throughLocalVariable} — accepted false-negative: store
 *       {@code record.offset()} in a {@code long} local and pass the local
 *       to {@code new OffsetAndMetadata(o)}. Even though this is the SAME
 *       off-by-one bug, the bytecode interposes {@code LSTORE} + {@code LLOAD}
 *       between the {@code offset()} call and the {@code <init>}, so the
 *       rule does NOT fire. We accept this false-negative deliberately to
 *       keep the false-positive rate at zero on legitimate code that
 *       computes the offset from another source (e.g. an external counter,
 *       a manually-tracked last-processed offset). This control documents
 *       the intentional gap.</li>
 * </ol>
 *
 * <p>Note method (7) is presented as a control even though it contains the
 * off-by-one bug semantically — the rule's structural detection is
 * conservative by design and trades the through-LSTORE shape for zero false
 * positives.
 */
public final class GoodConsumerCommitOffsetPlusOne {

    /** Correct: {@code record.offset() + 1} inline. */
    public void commitSyncPlusOneInline(Consumer<String, String> consumer, ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
        consumer.commitSync(offsets);
    }

    /** Correct: {@code + 1} with the metadata-string constructor. */
    public void commitSyncPlusOneMetadata(Consumer<String, String> consumer, ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(record.offset() + 1, "build=v1.4.2"));
        consumer.commitSync(offsets);
    }

    /** Correct: {@code + 1} with the leader-epoch (Kafka 2.4+) constructor. */
    public void commitSyncPlusOneLeaderEpoch(Consumer<String, String> consumer, ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        Optional<Integer> leaderEpoch = record.leaderEpoch();
        offsets.put(tp, new OffsetAndMetadata(record.offset() + 1, leaderEpoch, "epoch-aware"));
        consumer.commitSync(offsets);
    }

    /** Correct: {@code + 1} on the async path. */
    public void commitAsyncPlusOne(Consumer<String, String> consumer, ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
        consumer.commitAsync(offsets, null);
    }

    /** Correct: {@code + 1} on the EOS-v2 transactional commit path. */
    public void sendOffsetsToTransactionPlusOne(Producer<String, String> producer,
                                                 Consumer<String, String> consumer,
                                                 ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
    }

    /** Correct: batch-commit loop tracking {@code record.offset() + 1} per partition. */
    public void accumulatedOffsetsPlusOne(Consumer<String, String> consumer, ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            offsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
        }
        consumer.commitSync(offsets);
    }

    /**
     * Accepted false-negative: {@code long o = record.offset();
     * new OffsetAndMetadata(o)} — the bytecode interposes
     * {@code LSTORE} + {@code LLOAD} between the {@code offset()} call and
     * the {@code <init>}, so the rule's structural {@code prevSignificant}
     * check fails and the rule does NOT fire. This shape contains the SAME
     * off-by-one bug semantically; the rule trades the through-LSTORE shape
     * for zero false-positive risk on legitimate code that computes the
     * offset from an external source.
     */
    public void throughLocalVariable(Consumer<String, String> consumer, ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        long o = record.offset();
        offsets.put(tp, new OffsetAndMetadata(o));
        consumer.commitSync(offsets);
    }
}
