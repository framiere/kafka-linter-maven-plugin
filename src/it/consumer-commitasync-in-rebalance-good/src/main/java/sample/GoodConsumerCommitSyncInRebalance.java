package sample;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * Good shapes — must NOT fire CONSUMER_COMMITASYNC_IN_REBALANCE.
 *
 * <p>commitSync(currentOffsets) blocks the callback until the broker has the
 * offsets durably; when the callback returns the rebalance state machine can
 * safely proceed because the new owner will read from the just-committed offset.
 */
public final class GoodConsumerCommitSyncInRebalance implements ConsumerRebalanceListener {

    private final KafkaConsumer<String, String> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    public GoodConsumerCommitSyncInRebalance(
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        this.consumer = consumer;
        this.currentOffsets = currentOffsets;
    }

    /** Control #1: commitSync(currentOffsets) — safe, blocks until durable. */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitSync(currentOffsets);
    }

    /** Control #2: commitSync() no-arg — also blocks. */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        consumer.commitSync();
    }

    /** Control #3: no commit at all on assignment — typical shape. */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // nothing to do — consumer will read from last committed offset
    }
}
