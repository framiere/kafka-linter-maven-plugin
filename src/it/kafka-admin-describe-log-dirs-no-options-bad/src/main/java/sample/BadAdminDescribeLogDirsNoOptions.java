package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;

/**
 * RULE: ADMIN_DESCRIBE_LOG_DIRS_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeLogDirs(Collection<Integer>)} is called
 * without a {@code DescribeLogDirsOptions} argument. The call inherits the
 * default ~30 s AdminClient timeout — which is calibrated for fast
 * meta-operations (fetch metadata, alter configs) and is MISMATCHED to
 * disk-scan operations.
 *
 * <p>What the broker actually does on the wire: for each broker named in
 * the collection, enumerate every {@code log.dirs} entry; for each
 * directory walk every {@code TopicPartition} subdirectory, sum the log
 * segment file sizes, and read the head offset from the active segment's
 * index file. For a typical production broker (5 disks × 5000 partition
 * replicas × 10 segments per partition) that's 250K filesystem stat()
 * calls — 1-30 s on local NVMe, 30-120+ s on networked EBS-style storage.
 *
 * <p>Failure mode (capacity-planning dashboard blank on production
 * brokers):
 * <ol>
 *   <li>A Grafana dashboard refreshes every 5 minutes via
 *       {@code admin.describeLogDirs(allBrokerIds).all().get()}.</li>
 *   <li>Dev cluster (3 brokers × 200 partitions each) completes in 2 s.</li>
 *   <li>Production (12 brokers × 5000 partitions on EBS gp3) — the call
 *       hits the 30 s timeout on 4 of 12 brokers.</li>
 *   <li>The dashboard reports "capacity planning data unavailable for
 *       brokers 3, 7, 8, 11" until on-call traces the cause to the default
 *       timeout half a day later.</li>
 * </ol>
 *
 * <p>Fix: {@code new DescribeLogDirsOptions().timeoutMs(180_000)}
 * (3 minutes) — long enough for a disk scan even on contended large-disk
 * EBS hosts. Avoid running this at peak traffic hours if possible (the
 * scan competes with normal produce/fetch IOPS).
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Collection;)Lorg/apache/kafka/clients/admin/DescribeLogDirsResult;}
 *   <li>With options: {@code (Ljava/util/Collection;Lorg/apache/kafka/clients/admin/DescribeLogDirsOptions;)Lorg/apache/kafka/clients/admin/DescribeLogDirsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/DescribeLogDirsOptions;}
 * in the descriptor.
 */
public final class BadAdminDescribeLogDirsNoOptions {

    private static List<Integer> brokers() {
        return List.of(1, 2, 3);
    }

    /** Anti-pattern: no DescribeLogDirsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public DescribeLogDirsResult adminNoOptions(Admin admin) {
        return admin.describeLogDirs(brokers()); // FIRES — no options
    }

    /** Anti-pattern: no DescribeLogDirsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public DescribeLogDirsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeLogDirs(brokers()); // FIRES — no options
        }
    }

    /** Control: DescribeLogDirsOptions with disk-scan-appropriate timeout — must NOT fire. */
    public DescribeLogDirsResult adminWithOptions(Admin admin) {
        DescribeLogDirsOptions opts = new DescribeLogDirsOptions().timeoutMs(180_000);
        return admin.describeLogDirs(brokers(), opts);
    }
}
