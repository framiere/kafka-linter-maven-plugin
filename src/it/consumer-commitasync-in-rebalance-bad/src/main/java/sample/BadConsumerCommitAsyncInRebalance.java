package sample;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * Bad shapes — must fire CONSUMER_COMMITASYNC_IN_REBALANCE.
 *
 * <p>Each violation is a {@code commitAsync(...)} call inside either
 * {@code onPartitionsRevoked(Collection)} or {@code onPartitionsLost(Collection)}.
 * The rebalance state machine proceeds synchronously once the callback returns,
 * so the async commit posted in the body does NOT land before partitions are
 * reassigned — the next owner reads from the LAST PERSISTED offset and reprocesses.
 *
 * <p>{@code onPartitionsAssigned} contains a control: {@code commitAsync} there
 * is a DIFFERENT bug pattern (it can clobber the new owner's offsets) and the
 * rule deliberately ignores it.
 */
public final class BadConsumerCommitAsyncInRebalance implements ConsumerRebalanceListener {

    private final KafkaConsumer<String, String> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    public BadConsumerCommitAsyncInRebalance(
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        this.consumer = consumer;
        this.currentOffsets = currentOffsets;
    }

    /** Violation #1: commitAsync() no-arg inside onPartitionsRevoked. */
    /** Violation #2: commitAsync(Map, OffsetCommitCallback) inside onPartitionsRevoked. */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitAsync();
        consumer.commitAsync(currentOffsets, (offsets, exception) -> {
            if (exception != null) {
                System.err.println("commit failed: " + exception);
            }
        });
    }

    /** Violation #3: commitAsync(OffsetCommitCallback) inside onPartitionsLost. */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        consumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                System.err.println("lost-path commit failed: " + exception);
            }
        });
    }

    /** Control: commitAsync inside onPartitionsAssigned — must NOT fire (different bug pattern). */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        consumer.commitAsync();
    }
}
