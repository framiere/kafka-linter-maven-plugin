package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;

/**
 * RULE: ADMIN_DESCRIBE_TOPICS_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeTopics(Collection<String>)} is called without
 * a {@code DescribeTopicsOptions} argument. The no-options overload has TWO
 * silent defaults:
 *
 * <ol>
 *   <li><b>Timeout</b>: inherits {@code request.timeout.ms} (~30s).
 *   <li><b>{@code includeAuthorizedOperations}</b>: defaults to {@code false},
 *       so every {@code TopicDescription.authorizedOperations()} returns
 *       {@code null}. The broker has the ACL state; the no-options call just
 *       doesn't ask for it.
 * </ol>
 *
 * <p>Authorization-audit and migration tools relying on the no-options form
 * silently produce empty ACL inventories. The classic failure: an AD-to-SASL
 * migration script that calls {@code describeTopics(allTopics)}, sees
 * {@code authorizedOperations() == null}, writes 'no permissions, default deny'
 * for every topic, and breaks every consumer/producer on cutover.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Collection;)Lorg/apache/kafka/clients/admin/DescribeTopicsResult;}
 *   <li>With options: {@code (Ljava/util/Collection;Lorg/apache/kafka/clients/admin/DescribeTopicsOptions;)Lorg/apache/kafka/clients/admin/DescribeTopicsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/DescribeTopicsOptions;}
 * in the descriptor.
 */
public final class BadAdminDescribeTopicsNoOptions {

    private static List<String> topics() {
        return List.of("payments.events.v1", "payments.dlq.v1", "audit.events");
    }

    /** Anti-pattern: no DescribeTopicsOptions on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DescribeTopicsResult adminNoOptions(Admin admin) {
        return admin.describeTopics(topics()); // FIRES — no DescribeTopicsOptions
    }

    /** Anti-pattern: no DescribeTopicsOptions on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeTopicsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeTopics(topics()); // FIRES — no DescribeTopicsOptions
        }
    }

    /** Control: DescribeTopicsOptions with timeout AND includeAuthorizedOperations — must NOT fire. */
    public DescribeTopicsResult adminWithOptions(Admin admin) {
        DescribeTopicsOptions opts = new DescribeTopicsOptions()
                .timeoutMs(60_000)
                .includeAuthorizedOperations(true);
        return admin.describeTopics(topics(), opts);
    }
}
