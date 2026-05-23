package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;

/**
 * RULE: ADMIN_DESCRIBE_CLUSTER_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeCluster()} is called without a
 * {@code DescribeClusterOptions} argument. The no-options overload sets
 * {@code includeAuthorizedOperations=false} by default, so
 * {@code DescribeClusterResult.authorizedOperations().get()} returns
 * {@code null}.
 *
 * <p>Failure mode: a CI/CD pipeline runs a 'pre-deploy capability check'
 * with {@code admin.describeCluster().authorizedOperations().get()} to
 * verify the service principal has CREATE on the cluster. Without options,
 * the result is {@code null}; {@code ops.contains(AclOperation.CREATE)}
 * raises NPE; the catch block logs 'unable to determine permissions,
 * skipping' and proceeds; the subsequent {@code createTopics} call fails
 * mid-deploy with a {@code TopicAuthorizationException}.
 *
 * <p>Cluster-level ACLs live in the {@code CLUSTER} resource type — they
 * are NOT the same as per-topic ACLs returned by {@code describeTopics},
 * so even a tool that calls describeTopics-with-options is incomplete
 * without describeCluster-with-options.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code ()Lorg/apache/kafka/clients/admin/DescribeClusterResult;}
 *   <li>With options: {@code (Lorg/apache/kafka/clients/admin/DescribeClusterOptions;)Lorg/apache/kafka/clients/admin/DescribeClusterResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/DescribeClusterOptions;}
 * in the descriptor.
 */
public final class BadAdminDescribeClusterNoOptions {

    /** Anti-pattern: no DescribeClusterOptions on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DescribeClusterResult adminNoOptions(Admin admin) {
        return admin.describeCluster(); // FIRES — no DescribeClusterOptions
    }

    /** Anti-pattern: no DescribeClusterOptions on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeClusterResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeCluster(); // FIRES — no DescribeClusterOptions
        }
    }

    /** Control: DescribeClusterOptions with timeout AND includeAuthorizedOperations — must NOT fire. */
    public DescribeClusterResult adminWithOptions(Admin admin) {
        DescribeClusterOptions opts = new DescribeClusterOptions()
                .timeoutMs(60_000)
                .includeAuthorizedOperations(true);
        return admin.describeCluster(opts);
    }
}
