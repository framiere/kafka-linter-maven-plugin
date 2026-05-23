package sample;

import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Control for CONSUMER_SEEK_BEFORE_POLL.
 *
 * <p>Manual partition assignment shape: {@code assign()} synchronously
 * populates {@code assignment()} (no group coordinator round-trip is
 * needed), so a subsequent {@code seek()} in the same method has the
 * partition in its assignment set and works correctly at runtime.
 *
 * <p>The rule's state machine moves INITIAL → MANUAL_ASSIGN on the
 * assign() call, and seek() while MANUAL_ASSIGN is silent.
 *
 * <p>Lives in a separate class from {@link GoodConsumerSeekAfterPoll}
 * (which uses subscribe) so the class-scope
 * CONSUMER_ASSIGN_AND_SUBSCRIBE rule never sees both methods on the
 * same class.
 */
public final class GoodConsumerAssignThenSeek {

    public void replayFromOffset(Consumer<String, String> consumer,
                                 TopicPartition partition,
                                 long offset) {
        consumer.assign(List.of(partition));
        consumer.seek(partition, offset); // silent: state is MANUAL_ASSIGN
    }

    public void replayMultiplePartitions(Consumer<String, String> consumer,
                                         TopicPartition p0,
                                         TopicPartition p1) {
        consumer.assign(List.of(p0, p1));
        consumer.seek(p0, 0L); // silent
        consumer.seek(p1, 0L); // silent
    }
}
