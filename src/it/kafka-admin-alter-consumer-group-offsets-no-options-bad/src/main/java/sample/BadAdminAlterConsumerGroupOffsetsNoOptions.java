package sample;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: ADMIN_ALTER_CONSUMER_GROUP_OFFSETS_NO_OPTIONS.
 *
 * Fires when {@code Admin.alterConsumerGroupOffsets(String, Map<TopicPartition,
 * OffsetAndMetadata>)} is called without an
 * {@code AlterConsumerGroupOffsetsOptions} argument. This is the API behind
 * {@code kafka-consumer-groups --reset-offsets} — the most-used 'reset state'
 * operation in the Kafka ecosystem. The default ~30 s AdminClient timeout is
 * almost always too short for a multi-partition reset on a contended
 * {@code __consumer_offsets} coordinator, and the operation is BOTH
 * irreversible AND per-partition (no atomic rollback when the timeout fires).
 *
 * <p>Failure mode (half-reset Streams app, inconsistent state stores):
 * <ol>
 *   <li>A Streams app with 200 partitions hits a poison-pill record on
 *       Saturday; on Monday the ops team plans to reset to earliest, replay
 *       through Friday's fix, and resume.</li>
 *   <li>{@code admin.alterConsumerGroupOffsets(groupId, partitionToEarliest)
 *       .all().get()} hits the 30 s timeout at partition 113 of 200.</li>
 *   <li>Partitions 0-112 are now at offset 0; partitions 113-199 are still
 *       at their original Saturday offset. The catch block only logs
 *       'partial failure'.</li>
 *   <li>Restarting the Streams app, partition 0 replays from offset 0 while
 *       partition 199 continues from the poison-pill-corrupted state —
 *       state stores end up wildly inconsistent across the same key space.</li>
 * </ol>
 *
 * <p>Fix: {@code new AlterConsumerGroupOffsetsOptions().timeoutMs(120_000)}
 * AND inspect per-partition outcomes via
 * {@link AlterConsumerGroupOffsetsResult#partitionResult(TopicPartition)}
 * rather than the aggregated {@code all()} future.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/lang/String;Ljava/util/Map;)Lorg/apache/kafka/clients/admin/AlterConsumerGroupOffsetsResult;}
 *   <li>With options: {@code (Ljava/lang/String;Ljava/util/Map;Lorg/apache/kafka/clients/admin/AlterConsumerGroupOffsetsOptions;)Lorg/apache/kafka/clients/admin/AlterConsumerGroupOffsetsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/AlterConsumerGroupOffsetsOptions;}
 * in the descriptor.
 */
public final class BadAdminAlterConsumerGroupOffsetsNoOptions {

    private static final String GROUP = "payments-aggregator";

    private static Map<TopicPartition, OffsetAndMetadata> resetToEarliest() {
        return Map.of(
                new TopicPartition("payments.events", 0), new OffsetAndMetadata(0L),
                new TopicPartition("payments.events", 1), new OffsetAndMetadata(0L),
                new TopicPartition("payments.events", 2), new OffsetAndMetadata(0L));
    }

    /** Anti-pattern: no AlterConsumerGroupOffsetsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public AlterConsumerGroupOffsetsResult adminNoOptions(Admin admin) {
        return admin.alterConsumerGroupOffsets(GROUP, resetToEarliest()); // FIRES — no options
    }

    /** Anti-pattern: no AlterConsumerGroupOffsetsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public AlterConsumerGroupOffsetsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.alterConsumerGroupOffsets(GROUP, resetToEarliest()); // FIRES — no options
        }
    }

    /** Control: AlterConsumerGroupOffsetsOptions with generous timeout — must NOT fire. */
    public AlterConsumerGroupOffsetsResult adminWithOptions(Admin admin) {
        AlterConsumerGroupOffsetsOptions opts = new AlterConsumerGroupOffsetsOptions().timeoutMs(120_000);
        return admin.alterConsumerGroupOffsets(GROUP, resetToEarliest(), opts);
    }
}
