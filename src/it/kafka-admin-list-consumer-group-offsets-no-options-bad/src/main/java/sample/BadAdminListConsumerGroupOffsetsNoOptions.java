package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: ADMIN_LIST_CONSUMER_GROUP_OFFSETS_NO_OPTIONS.
 *
 * Fires when {@code Admin.listConsumerGroupOffsets(String groupId)} is called
 * without a {@code ListConsumerGroupOffsetsOptions} argument. Without
 * {@code topicPartitions} filter, the broker returns the committed offset for
 * EVERY topic-partition the group has EVER committed to within
 * {@code offsets.retention.minutes} (default 7 days).
 *
 * <p>Failure mode: an offset-reset CLI calls
 * {@code admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()}
 * to inventory current offsets before resetting. The group ran for 2 years
 * through three migration phases — historical commits exist for 12 topics ×
 * 50 partitions = 600 partitions, retained because migrations were recent.
 * The current subscription is just 1 topic × 50 partitions. Without
 * {@code topicPartitions(currentPartitions)}, the call returns 600 entries;
 * the reset issues 600 {@code alterConsumerGroupOffsets} operations, 550 of
 * which are for partitions the group no longer cares about — bloating
 * {@code __consumer_offsets} and triggering unnecessary rebalance probes.
 *
 * <p>The {@code topicPartitions} filter exists since Kafka 3.0 (KIP-709)
 * specifically to address this historical-offset accumulation problem.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/lang/String;)Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsResult;}
 *   <li>With options: {@code (Ljava/lang/String;Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsOptions;)Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsOptions;}
 * in the descriptor.
 */
public final class BadAdminListConsumerGroupOffsetsNoOptions {

    private static final String GROUP_ID = "payments-aggregator-v3";

    /** Anti-pattern: no ListConsumerGroupOffsetsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public ListConsumerGroupOffsetsResult adminNoOptions(Admin admin) {
        return admin.listConsumerGroupOffsets(GROUP_ID); // FIRES — no options
    }

    /** Anti-pattern: no ListConsumerGroupOffsetsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public ListConsumerGroupOffsetsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.listConsumerGroupOffsets(GROUP_ID); // FIRES — no options
        }
    }

    /** Control: ListConsumerGroupOffsetsOptions with explicit topicPartitions filter — must NOT fire. */
    public ListConsumerGroupOffsetsResult adminWithOptions(Admin admin) {
        ListConsumerGroupOffsetsOptions opts = new ListConsumerGroupOffsetsOptions()
                .timeoutMs(60_000)
                .topicPartitions(List.of(
                        new TopicPartition("payments.events", 0),
                        new TopicPartition("payments.events", 1),
                        new TopicPartition("payments.events", 2)));
        return admin.listConsumerGroupOffsets(GROUP_ID, opts);
    }
}
