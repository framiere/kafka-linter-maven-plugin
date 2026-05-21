package sample;

import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: ADMIN_DELETE_CONSUMER_GROUP_OFFSETS_NO_OPTIONS.
 *
 * Fires when {@code Admin.deleteConsumerGroupOffsets(groupId, Set<TopicPartition>)} is called
 * without a {@code DeleteConsumerGroupOffsetsOptions} parameter. The bytecode descriptor of
 * the no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/DeleteConsumerGroupOffsetsOptions;}.
 */
public final class BadAdminDeleteConsumerGroupOffsetsNoOptions {

    private static Set<TopicPartition> partitions() {
        return Set.of(new TopicPartition("orders", 0), new TopicPartition("orders", 1));
    }

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DeleteConsumerGroupOffsetsResult adminNoOptions(Admin admin) {
        return admin.deleteConsumerGroupOffsets("payments", partitions()); // FIRES — no DeleteConsumerGroupOffsetsOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DeleteConsumerGroupOffsetsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.deleteConsumerGroupOffsets("payments", partitions()); // FIRES — no DeleteConsumerGroupOffsetsOptions
        }
    }

    /** Control: explicit DeleteConsumerGroupOffsetsOptions with long timeout — must NOT fire. */
    public DeleteConsumerGroupOffsetsResult adminWithOptions(Admin admin) {
        DeleteConsumerGroupOffsetsOptions opts = new DeleteConsumerGroupOffsetsOptions().timeoutMs(120_000);
        return admin.deleteConsumerGroupOffsets("payments", partitions(), opts);
    }
}
