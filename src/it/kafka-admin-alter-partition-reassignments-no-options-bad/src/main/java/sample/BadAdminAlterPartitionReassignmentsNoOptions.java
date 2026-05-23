package sample;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: ADMIN_ALTER_PARTITION_REASSIGNMENTS_NO_OPTIONS.
 *
 * Fires when {@code Admin.alterPartitionReassignments(Map<TopicPartition,
 * Optional<NewPartitionReassignment>>)} is called without an
 * {@code AlterPartitionReassignmentsOptions} argument. This is the KIP-455
 * (Kafka 2.4+) replacement for the old ZooKeeper reassignment-tool, and the
 * foundation of every Kafka rebalancing workflow: Cruise Control, Strimzi's
 * KafkaRebalance reconciler, Confluent Self-Balancing Clusters, and every
 * operator runbook for capacity moves.
 *
 * <p>What's confusing about the default timeout: the AdminClient future
 * completes when the CONTROLLER acks the metadata WRITE — NOT when the
 * data copy completes. Even the ack can take longer than 30 s on a busy
 * cluster because the controller serializes admin writes, queues them
 * behind any in-flight Cruise Control moves, and acks only after the
 * metadata log replicates the change to a majority. The data copy
 * (potentially terabytes across the network) proceeds asynchronously
 * regardless.
 *
 * <p>Failure mode (Cruise Control wrapper retries a reassignment that
 * already succeeded):
 * <ol>
 *   <li>Operator's wrapper calls
 *       {@code admin.alterPartitionReassignments(plan).all().get()} on a
 *       heavily-loaded production cluster.</li>
 *   <li>The controller takes 35 s to ack the write; the AdminClient times
 *       out at 30 s; the wrapper's catch block retries.</li>
 *   <li>The FIRST request already succeeded — the controller refuses the
 *       duplicate with {@code INVALID_REPLICA_ASSIGNMENT} because those
 *       partitions are now being reassigned.</li>
 *   <li>The wrapper logs 'reassignment failed' but the actual reassignment
 *       from the first call completes 2 hours later. The operator wastes
 *       45 minutes investigating a phantom failure.</li>
 * </ol>
 *
 * <p>Fix: {@code new AlterPartitionReassignmentsOptions().timeoutMs(120_000)}
 * for the initial ack, then poll {@code admin.listPartitionReassignments()}
 * to observe actual data-copy progress.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Map;)Lorg/apache/kafka/clients/admin/AlterPartitionReassignmentsResult;}
 *   <li>With options: {@code (Ljava/util/Map;Lorg/apache/kafka/clients/admin/AlterPartitionReassignmentsOptions;)Lorg/apache/kafka/clients/admin/AlterPartitionReassignmentsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/AlterPartitionReassignmentsOptions;}
 * in the descriptor.
 */
public final class BadAdminAlterPartitionReassignmentsNoOptions {

    private static Map<TopicPartition, Optional<NewPartitionReassignment>> plan() {
        return Map.of(
                new TopicPartition("payments.events", 0),
                Optional.of(new NewPartitionReassignment(List.of(1, 2, 3))),
                new TopicPartition("payments.events", 1),
                Optional.of(new NewPartitionReassignment(List.of(2, 3, 4))));
    }

    /** Anti-pattern: no AlterPartitionReassignmentsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public AlterPartitionReassignmentsResult adminNoOptions(Admin admin) {
        return admin.alterPartitionReassignments(plan()); // FIRES — no options
    }

    /** Anti-pattern: no AlterPartitionReassignmentsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public AlterPartitionReassignmentsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.alterPartitionReassignments(plan()); // FIRES — no options
        }
    }

    /** Control: AlterPartitionReassignmentsOptions with generous timeout — must NOT fire. */
    public AlterPartitionReassignmentsResult adminWithOptions(Admin admin) {
        AlterPartitionReassignmentsOptions opts = new AlterPartitionReassignmentsOptions().timeoutMs(120_000);
        return admin.alterPartitionReassignments(plan(), opts);
    }
}
