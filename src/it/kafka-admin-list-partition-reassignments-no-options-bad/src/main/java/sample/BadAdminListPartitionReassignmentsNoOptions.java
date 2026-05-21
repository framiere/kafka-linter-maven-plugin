package sample;

import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: ADMIN_LIST_PARTITION_REASSIGNMENTS_NO_OPTIONS.
 *
 * Fires when {@code Admin.listPartitionReassignments()} is called without a
 * {@code ListPartitionReassignmentsOptions} parameter. The bytecode descriptor
 * of the no-options overloads (0-arg and 1-arg-Set) does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/ListPartitionReassignmentsOptions;}.
 */
public final class BadAdminListPartitionReassignmentsNoOptions {

    /** Anti-pattern: 0-arg form on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public ListPartitionReassignmentsResult adminNoArgNoOptions(Admin admin) {
        return admin.listPartitionReassignments(); // FIRES — no ListPartitionReassignmentsOptions
    }

    /** Anti-pattern: 1-arg-Set form on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public ListPartitionReassignmentsResult adminSetNoOptions(Admin admin, Set<TopicPartition> partitions) {
        return admin.listPartitionReassignments(partitions); // FIRES — no ListPartitionReassignmentsOptions
    }

    /** Anti-pattern: 0-arg form on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public ListPartitionReassignmentsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.listPartitionReassignments(); // FIRES — no ListPartitionReassignmentsOptions
        }
    }

    /** Control: explicit ListPartitionReassignmentsOptions with long timeout — must NOT fire. */
    public ListPartitionReassignmentsResult adminWithOptions(Admin admin, Set<TopicPartition> partitions) {
        ListPartitionReassignmentsOptions opts = new ListPartitionReassignmentsOptions().timeoutMs(120_000);
        return admin.listPartitionReassignments(partitions, opts);
    }
}
