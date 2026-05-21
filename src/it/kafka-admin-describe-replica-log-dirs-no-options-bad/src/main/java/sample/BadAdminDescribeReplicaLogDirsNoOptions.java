package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.TopicPartitionReplica;

/**
 * RULE: ADMIN_DESCRIBE_REPLICA_LOG_DIRS_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeReplicaLogDirs(Collection<TopicPartitionReplica>)}
 * is called without a {@code DescribeReplicaLogDirsOptions} parameter. The bytecode
 * descriptor of the no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/DescribeReplicaLogDirsOptions;}.
 */
public final class BadAdminDescribeReplicaLogDirsNoOptions {

    private static List<TopicPartitionReplica> replicas() {
        return List.of(
                new TopicPartitionReplica("payments", 0, 3),
                new TopicPartitionReplica("payments", 1, 3),
                new TopicPartitionReplica("orders", 0, 3));
    }

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DescribeReplicaLogDirsResult adminNoOptions(Admin admin) {
        return admin.describeReplicaLogDirs(replicas()); // FIRES — no DescribeReplicaLogDirsOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeReplicaLogDirsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeReplicaLogDirs(replicas()); // FIRES — no DescribeReplicaLogDirsOptions
        }
    }

    /** Control: explicit DescribeReplicaLogDirsOptions with long timeout — must NOT fire. */
    public DescribeReplicaLogDirsResult adminWithOptions(Admin admin) {
        DescribeReplicaLogDirsOptions opts = new DescribeReplicaLogDirsOptions().timeoutMs(120_000);
        return admin.describeReplicaLogDirs(replicas(), opts);
    }
}
