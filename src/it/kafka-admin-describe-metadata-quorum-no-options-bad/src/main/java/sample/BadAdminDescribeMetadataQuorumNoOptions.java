package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;

/**
 * RULE: ADMIN_DESCRIBE_METADATA_QUORUM_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeMetadataQuorum()} is called without a
 * {@code DescribeMetadataQuorumOptions} parameter. The bytecode descriptor of
 * the no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/DescribeMetadataQuorumOptions;}.
 */
public final class BadAdminDescribeMetadataQuorumNoOptions {

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DescribeMetadataQuorumResult adminNoOptions(Admin admin) {
        return admin.describeMetadataQuorum(); // FIRES — no DescribeMetadataQuorumOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeMetadataQuorumResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeMetadataQuorum(); // FIRES — no DescribeMetadataQuorumOptions
        }
    }

    /** Control: explicit DescribeMetadataQuorumOptions with long timeout — must NOT fire. */
    public DescribeMetadataQuorumResult adminWithOptions(Admin admin) {
        DescribeMetadataQuorumOptions opts = new DescribeMetadataQuorumOptions().timeoutMs(120_000);
        return admin.describeMetadataQuorum(opts);
    }
}
