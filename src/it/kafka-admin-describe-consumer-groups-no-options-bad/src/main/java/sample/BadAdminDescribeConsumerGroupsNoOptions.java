package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;

/**
 * RULE: ADMIN_DESCRIBE_CONSUMER_GROUPS_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeConsumerGroups(Collection<String>)} is
 * called without a {@code DescribeConsumerGroupsOptions} argument. The
 * no-options overload sets {@code includeAuthorizedOperations=false} by
 * default, so {@code ConsumerGroupDescription.authorizedOperations()} returns
 * {@code null} on every entry.
 *
 * <p>Failure mode: a service-deployment pipeline calls
 * {@code admin.describeConsumerGroups(List.of(myGroup)).all().get().get(myGroup).authorizedOperations()}
 * as a pre-deploy capability check for READ on the consumer group. Without
 * {@code includeAuthorizedOperations(true)}, the returned set is {@code null};
 * the calling code's
 * {@code Optional.ofNullable(ops).map(s -> s.contains(READ)).orElse(false)}
 * evaluates to {@code false}; the pipeline aborts the deploy claiming 'no
 * permission'; an SRE spends two hours investigating an ACL config that's
 * actually correct.
 *
 * <p>Group-level ACLs are distinct from per-topic ACLs — a service can have
 * READ on its source topics but lack READ on the consumer group, or vice
 * versa, so the group-level capability check is a meaningful pre-deploy step.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Collection;)Lorg/apache/kafka/clients/admin/DescribeConsumerGroupsResult;}
 *   <li>With options: {@code (Ljava/util/Collection;Lorg/apache/kafka/clients/admin/DescribeConsumerGroupsOptions;)Lorg/apache/kafka/clients/admin/DescribeConsumerGroupsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/DescribeConsumerGroupsOptions;}
 * in the descriptor.
 */
public final class BadAdminDescribeConsumerGroupsNoOptions {

    private static List<String> groups() {
        return List.of("payments-aggregator", "audit-publisher", "fraud-scorer");
    }

    /** Anti-pattern: no DescribeConsumerGroupsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public DescribeConsumerGroupsResult adminNoOptions(Admin admin) {
        return admin.describeConsumerGroups(groups()); // FIRES — no options
    }

    /** Anti-pattern: no DescribeConsumerGroupsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public DescribeConsumerGroupsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeConsumerGroups(groups()); // FIRES — no options
        }
    }

    /** Control: DescribeConsumerGroupsOptions with timeout + includeAuthorizedOperations — must NOT fire. */
    public DescribeConsumerGroupsResult adminWithOptions(Admin admin) {
        DescribeConsumerGroupsOptions opts = new DescribeConsumerGroupsOptions()
                .timeoutMs(60_000)
                .includeAuthorizedOperations(true);
        return admin.describeConsumerGroups(groups(), opts);
    }
}
