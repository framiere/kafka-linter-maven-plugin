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
 * RULE: CONSUMER_COMMIT_OFFSET_OFF_BY_ONE.
 *
 * <p>Fires when {@code new OffsetAndMetadata(record.offset())} is used as the
 * commit value instead of {@code new OffsetAndMetadata(record.offset() + 1)}.
 * The kafka-clients commit contract is unambiguous: the committed offset is
 * the offset the consumer should READ NEXT, not the offset of the record it
 * just processed. The committed value is what the broker stores in
 * {@code __consumer_offsets}; on the next process restart, rebalance, or
 * partition reassignment, the consumer's starting position is set to that
 * exact value — so committing {@code record.offset()} means the record at
 * that offset is re-delivered, and at-least-once silently becomes
 * at-least-twice for the last record of every committed batch.
 *
 * <p>The bug is invisible during steady-state operation. While the JVM is
 * running, the consumer's in-memory {@code position} advances correctly to
 * {@code lastReturnedOffset + 1} on every {@code poll()}, independent of
 * what the application later commits. So as long as the consumer keeps
 * running, the off-by-one in the commit is undetectable — it only manifests
 * when another consumer reads {@code __consumer_offsets} for the starting
 * position. Detection at build time is the only sensible point of
 * intervention.
 *
 * <p>What this rule catches (six method-scope shapes, exercising each of
 * the three relevant {@code OffsetAndMetadata} constructors and both
 * {@code commitSync(Map)} / {@code commitAsync(Map)} call sites, plus the
 * transactional EOS path {@code sendOffsetsToTransaction}):
 * <ol>
 *   <li>{@code commitSyncOffsetOnly} — {@code new OffsetAndMetadata(record.offset())}
 *       passed to {@code commitSync(Map)} in a poll loop. The most common
 *       shape; copy-paste from tutorials.</li>
 *   <li>{@code commitSyncOffsetWithMetadata} — same off-by-one with the
 *       {@code (J, String)} two-arg constructor. The metadata-string overload
 *       is used to attach diagnostic context (host, build version) to
 *       offsets; the off-by-one in the offset argument is identical.</li>
 *   <li>{@code commitSyncOffsetWithLeaderEpoch} — Kafka 2.4+
 *       {@code (J, Optional&lt;Integer&gt;, String)} three-arg constructor with
 *       leader-epoch fencing. Same off-by-one on the offset argument.</li>
 *   <li>{@code commitAsyncOffByOne} — {@code commitAsync(Map)} variant with
 *       {@code new OffsetAndMetadata(record.offset())} in the map values.
 *       The async path does not change the commit semantics; the off-by-one
 *       is identical to the sync version.</li>
 *   <li>{@code sendOffsetsToTransactionOffByOne} — EOS-v2 path
 *       {@code producer.sendOffsetsToTransaction(Map, ConsumerGroupMetadata)}
 *       with off-by-one offset. Breaks the EOS contract from the application
 *       side — the transactional commit atomicity is fine but the offset
 *       stored is wrong, so every restart of the transactional consumer
 *       re-processes the last record.</li>
 *   <li>{@code accumulatedOffsetsOffByOne} — a typical batch-commit loop
 *       that walks {@code ConsumerRecords} and stores the last offset per
 *       partition into a {@code Map} for a single bulk commit at the end of
 *       the poll cycle. The off-by-one is on the {@code put} that constructs
 *       the {@code OffsetAndMetadata}.</li>
 * </ol>
 *
 * <p>Correct pattern: every {@code new OffsetAndMetadata(offset, ...)} must
 * pass {@code record.offset() + 1} (or the equivalent {@code highestOffset
 * + 1} when batching). The {@code GoodConsumerCommitOffsetPlusOne} silent
 * controls cover the canonical correct shapes.
 */
public final class BadConsumerCommitOffsetOffByOne {

    /**
     * Anti-pattern #1: {@code new OffsetAndMetadata(record.offset())}
     * passed to {@code commitSync(Map)}. The committed offset is the offset
     * of the JUST-PROCESSED record, not the next-to-fetch offset — every
     * consumer restart or rebalance re-delivers the record at this offset.
     */
    public void commitSyncOffsetOnly(Consumer<String, String> consumer, ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(record.offset())); // FIRES — off-by-one (should be record.offset() + 1)
        consumer.commitSync(offsets);
    }

    /**
     * Anti-pattern #2: same off-by-one shape with the
     * {@code (J, String)} two-arg constructor that attaches a metadata
     * string to the commit. The metadata argument is irrelevant to the bug
     * — the first long argument is still the off-by-one offset.
     */
    public void commitSyncOffsetWithMetadata(Consumer<String, String> consumer, ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(record.offset(), "build=v1.4.2")); // FIRES — off-by-one
        consumer.commitSync(offsets);
    }

    /**
     * Anti-pattern #3: Kafka 2.4+
     * {@code (J, Optional&lt;Integer&gt;, String)} three-arg constructor for
     * leader-epoch fencing. The leader-epoch argument fixes a different
     * problem (zombie leaders); the offset argument is still off-by-one.
     */
    public void commitSyncOffsetWithLeaderEpoch(Consumer<String, String> consumer, ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        Optional<Integer> leaderEpoch = record.leaderEpoch();
        offsets.put(tp, new OffsetAndMetadata(record.offset(), leaderEpoch, "epoch-aware")); // FIRES — off-by-one
        consumer.commitSync(offsets);
    }

    /**
     * Anti-pattern #4: {@code commitAsync(Map)} variant. The async path
     * does not change the commit semantics — the OffsetCommitRequest still
     * stores the off-by-one offset in {@code __consumer_offsets}; restarts
     * re-deliver the last record exactly the same way.
     */
    public void commitAsyncOffByOne(Consumer<String, String> consumer, ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(record.offset())); // FIRES — off-by-one
        consumer.commitAsync(offsets, null);
    }

    /**
     * Anti-pattern #5: EOS-v2 transactional commit path
     * {@code producer.sendOffsetsToTransaction(Map, ConsumerGroupMetadata)}.
     * The transactional atomicity is intact (the commit happens iff the
     * producer transaction commits) but the offset value stored is still
     * off-by-one — so every restart of the transactional consumer
     * re-processes the last record of each committed transaction.
     */
    public void sendOffsetsToTransactionOffByOne(Producer<String, String> producer,
                                                  Consumer<String, String> consumer,
                                                  ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(record.offset())); // FIRES — off-by-one
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
    }

    /**
     * Anti-pattern #6: batch-commit loop that walks the
     * {@code ConsumerRecords} and stores the last offset per partition into
     * a {@code Map}. The natural shape developers write inside the per-record
     * loop is {@code new OffsetAndMetadata(record.offset())} (intending
     * 'remember the last processed offset for this partition') — but the
     * commit semantics require {@code + 1}. Same off-by-one, same silent
     * duplicate-on-restart.
     */
    public void accumulatedOffsetsOffByOne(Consumer<String, String> consumer, ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            offsets.put(tp, new OffsetAndMetadata(record.offset())); // FIRES — off-by-one
        }
        consumer.commitSync(offsets);
    }
}
