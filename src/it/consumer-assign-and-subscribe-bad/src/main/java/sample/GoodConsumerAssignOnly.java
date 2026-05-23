package sample;

import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Control for CONSUMER_ASSIGN_AND_SUBSCRIBE.
 *
 * <p>Self-managed shape: only assign() — no subscribe() anywhere in
 * the class. Typical of admin-style or backfill consumers that pick
 * specific partitions and don't participate in a consumer group.
 * The class-scope rule sees assign but never finds a subscribe site,
 * so the violation list stays empty.
 */
public final class GoodConsumerAssignOnly {

    public void pickSpecificPartitions(Consumer<String, String> consumer,
                                       TopicPartition p0,
                                       TopicPartition p1) {
        consumer.assign(List.of(p0, p1));
    }

    public void switchToDifferentPartitions(Consumer<String, String> consumer,
                                            TopicPartition p2) {
        consumer.assign(List.of(p2));
    }
}
