package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: ADMIN_DESCRIBE_PRODUCERS_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeProducers(Collection<TopicPartition>)} is called
 * without a {@code DescribeProducersOptions} parameter. The bytecode descriptor of
 * the no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/DescribeProducersOptions;}.
 */
public final class BadAdminDescribeProducersNoOptions {

    private static List<TopicPartition> partitions() {
        return List.of(
                new TopicPartition("payments", 0),
                new TopicPartition("payments", 1),
                new TopicPartition("orders", 0));
    }

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DescribeProducersResult adminNoOptions(Admin admin) {
        return admin.describeProducers(partitions()); // FIRES — no DescribeProducersOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeProducersResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeProducers(partitions()); // FIRES — no DescribeProducersOptions
        }
    }

    /** Control: explicit DescribeProducersOptions with long timeout — must NOT fire. */
    public DescribeProducersResult adminWithOptions(Admin admin) {
        DescribeProducersOptions opts = new DescribeProducersOptions().timeoutMs(120_000);
        return admin.describeProducers(partitions(), opts);
    }
}
