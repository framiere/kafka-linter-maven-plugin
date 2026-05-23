package sample;

import java.util.Collection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * Control for POLL_IN_REBALANCE_CALLBACK.
 *
 * <p>Class implements {@code ConsumerRebalanceListener} (so the rule
 * does scan it — the first guard passes) but the three callback
 * methods only call {@code commitSync}, {@code seek}, and
 * {@code position} — NOT {@code poll}. The rule's inner check is
 * scoped to MethodInsnNode whose name is {@code "poll"}, so every
 * other consumer method is silently ignored. Violation list stays
 * empty.
 *
 * <p>These are the canonical uses of the rebalance callbacks:
 * <ul>
 *   <li>{@code onPartitionsRevoked}: commitSync the latest processed
 *       offsets so the next owner of these partitions starts where
 *       this consumer left off.</li>
 *   <li>{@code onPartitionsAssigned}: seek to externally-stored
 *       offsets (e.g. from a database) — assignment is fully
 *       populated here, so seek is safe.</li>
 *   <li>{@code onPartitionsLost}: read-only position() lookup to
 *       record what was lost; offsets cannot be committed (the
 *       broker has already taken the partitions away).</li>
 * </ul>
 */
public final class GoodCommitSyncInRebalanceCallback implements ConsumerRebalanceListener {

    private final Consumer<String, String> consumer;

    public GoodCommitSyncInRebalanceCallback(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitSync(); // silent: not poll
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            long stored = loadExternalOffset(tp);
            consumer.seek(tp, stored); // silent: not poll
        }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            long pos = consumer.position(tp); // silent: not poll
            recordLostPosition(tp, pos);
        }
    }

    private long loadExternalOffset(TopicPartition tp) {
        return 0L;
    }

    private void recordLostPosition(TopicPartition tp, long pos) {
        // no-op for the lint fixture
    }
}
