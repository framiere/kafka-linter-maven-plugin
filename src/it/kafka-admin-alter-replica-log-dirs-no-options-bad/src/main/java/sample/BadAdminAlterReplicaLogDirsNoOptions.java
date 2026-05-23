package sample;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.common.TopicPartitionReplica;

/**
 * RULE: ADMIN_ALTER_REPLICA_LOG_DIRS_NO_OPTIONS.
 *
 * Fires when {@code Admin.alterReplicaLogDirs(Map<TopicPartitionReplica,
 * String>)} is called without an {@code AlterReplicaLogDirsOptions}
 * argument. This is KIP-113 (Kafka 1.1+) — INTRA-broker log-directory
 * migration. For each named replica, the broker copies the partition's log
 * segments from whatever log directory currently hosts them to the named
 * target directory on the SAME broker, then atomically switches. This is
 * the API behind every 'rebalance disks within a broker' workflow:
 * Cruise Control's intra-broker balancer, Confluent's tiered-storage local
 * migration, every operator script for 'one disk full, the others empty'.
 *
 * <p>Asynchronous-completion-with-synchronous-ack quirk: the broker
 * enqueues the move on its {@code ReplicaAlterLogDirsManager} thread and
 * the {@code AlterReplicaLogDirsResponse} returns when the move is
 * ENQUEUED — not when it completes. The AdminClient still waits up to the
 * configured timeout for that initial ack, however. On a heavily-loaded
 * broker the ack itself can be delayed past the default ~30 s, even though
 * the actual segment-by-segment copy will run for minutes-to-hours
 * afterwards.
 *
 * <p>Failure mode (disk-rebalance script reports failure for successful
 * moves):
 * <ol>
 *   <li>An SRE wraps {@code admin.alterReplicaLogDirs(moves).all().get()}
 *       (no options) into a Python tool to move 50 large replicas off a
 *       full disk.</li>
 *   <li>Staging (4 GB partitions on local SSD) — completes in 18 s.</li>
 *   <li>Production (1.5 TB partitions on networked storage, with concurrent
 *       Cruise Control moves) — the ack queues behind other moves; the
 *       AdminClient times out at 30 s; the tool reports
 *       'disk rebalance failed: TimeoutException'.</li>
 *   <li>Two hours later, the migrations have actually completed on the
 *       broker (visible via {@code describeLogDirs}). Operators no longer
 *       trust the tool's success/failure reporting.</li>
 * </ol>
 *
 * <p>Fix: {@code new AlterReplicaLogDirsOptions().timeoutMs(120_000)} for
 * the initial ack PLUS a {@code describeLogDirs}-based completion-polling
 * loop (poll the {@code futureLogDir} field — when it disappears the
 * migration is done).
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Map;)Lorg/apache/kafka/clients/admin/AlterReplicaLogDirsResult;}
 *   <li>With options: {@code (Ljava/util/Map;Lorg/apache/kafka/clients/admin/AlterReplicaLogDirsOptions;)Lorg/apache/kafka/clients/admin/AlterReplicaLogDirsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/AlterReplicaLogDirsOptions;}
 * in the descriptor.
 */
public final class BadAdminAlterReplicaLogDirsNoOptions {

    private static Map<TopicPartitionReplica, String> moves() {
        // Move two partition replicas on broker 1 from disk0 to disk1.
        return Map.of(
                new TopicPartitionReplica("payments.events", 0, 1), "/var/lib/kafka/disk1",
                new TopicPartitionReplica("payments.events", 1, 1), "/var/lib/kafka/disk1");
    }

    /** Anti-pattern: no AlterReplicaLogDirsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public AlterReplicaLogDirsResult adminNoOptions(Admin admin) {
        return admin.alterReplicaLogDirs(moves()); // FIRES — no options
    }

    /** Anti-pattern: no AlterReplicaLogDirsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public AlterReplicaLogDirsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.alterReplicaLogDirs(moves()); // FIRES — no options
        }
    }

    /** Control: AlterReplicaLogDirsOptions with generous timeout — must NOT fire. */
    public AlterReplicaLogDirsResult adminWithOptions(Admin admin) {
        AlterReplicaLogDirsOptions opts = new AlterReplicaLogDirsOptions().timeoutMs(120_000);
        return admin.alterReplicaLogDirs(moves(), opts);
    }
}
