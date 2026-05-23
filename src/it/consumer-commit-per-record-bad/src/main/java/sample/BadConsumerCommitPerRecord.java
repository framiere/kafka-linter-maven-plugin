package sample;

import java.time.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * RULE: CONSUMER_COMMIT_PER_RECORD.
 *
 * <p>Fires when {@code commitSync()} (or {@code commitSync(...)}) sits
 * INSIDE the per-record loop of a {@code poll()} batch — i.e. on every
 * single record instead of after the inner-loop completes. This is the
 * "commit after every message" pattern that intuitively looks safe
 * (you commit what you just processed) but in practice is one of the
 * single largest avoidable consumer throughput regressions.
 *
 * <p>What it actually costs:
 * <ul>
 *   <li>{@code commitSync()} is a SYNCHRONOUS round-trip to the group
 *       coordinator. At default settings (50ms p50, 200ms p99 in
 *       healthy clusters), every record now pays a coordinator-RTT
 *       cost on its critical path.</li>
 *   <li>A consumer pulling 500 records per {@code poll()} now does
 *       500 synchronous commits per batch — at ~50ms each, that's
 *       25 SECONDS of pure commit-RTT to process what should have
 *       been ~100 ms of actual work.</li>
 *   <li>The coordinator broker is the bottleneck. A single
 *       commit-per-record consumer can saturate the
 *       coordinator's IO threads, increasing tail latency for
 *       OTHER consumer groups on the same broker.</li>
 *   <li>Worse: {@code __consumer_offsets} grows linearly with commits.
 *       A 10kreq/sec consumer commits 10kreq/sec into the offsets
 *       topic, multiplying its retention/compaction cost by 500×
 *       versus per-batch commits.</li>
 * </ul>
 *
 * <p>What the correct pattern looks like:
 * <pre>{@code
 *   while (running) {
 *       ConsumerRecords<K, V> recs = consumer.poll(Duration.ofMillis(500));
 *       for (ConsumerRecord<K, V> r : recs) {
 *           process(r);
 *       }
 *       consumer.commitSync();          // ← OUTSIDE the inner loop
 *   }
 * }</pre>
 *
 * <p>Detection: the rule walks bytecode looking for {@code commitSync}
 * calls that sit inside an INNER back-edge (a jump whose target is a
 * LABEL that appears AFTER the first {@code poll()} call in the
 * method). The outer {@code while (running)} loop's back-edge targets
 * a label BEFORE poll, so a commit between iterations of the outer
 * loop is correctly classified as per-BATCH and does NOT fire.
 *
 * <p>Variants covered:
 * <ul>
 *   <li>{@code commitSync()} on the {@link Consumer} interface
 *       (INVOKEINTERFACE) inside a for-each over
 *       {@link ConsumerRecords}.</li>
 *   <li>{@code commitSync()} on the {@link KafkaConsumer} concrete
 *       class (INVOKEVIRTUAL) inside the same shape.</li>
 *   <li>Control: {@code commitSync()} placed AFTER the inner loop
 *       but still inside the outer poll loop — must NOT fire.</li>
 * </ul>
 */
public final class BadConsumerCommitPerRecord {

    /** Anti-pattern: commitSync inside per-record loop on Consumer interface — FIRES. */
    public void commitPerRecordInterface(Consumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                process(r);
                consumer.commitSync(); // FIRES — coordinator RTT per record
            }
        }
    }

    /** Anti-pattern: commitSync inside per-record loop on KafkaConsumer concrete — FIRES. */
    public void commitPerRecordConcrete(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                process(r);
                consumer.commitSync(); // FIRES — coordinator RTT per record
            }
        }
    }

    /** Control: commitSync AFTER the inner loop (per-batch) — must NOT fire. */
    public void commitPerBatch(Consumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                process(r);
            }
            consumer.commitSync();
        }
    }

    private void process(ConsumerRecord<String, String> r) {
        // Imagine work.
        if (r.key() == null && r.value() == null) {
            throw new IllegalStateException("empty record");
        }
    }
}
