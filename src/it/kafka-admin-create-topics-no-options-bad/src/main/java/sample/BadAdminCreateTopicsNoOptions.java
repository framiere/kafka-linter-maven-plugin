package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * RULE: ADMIN_CREATE_TOPICS_NO_OPTIONS.
 *
 * Fires when {@code Admin.createTopics(Collection<NewTopic>)} is called without a
 * {@code CreateTopicsOptions} argument. The no-options overload uses the default
 * {@code request.timeout.ms} (~30s) — a TimeoutException does NOT mean the topic
 * was not created (the broker may have created it just after the client gave up),
 * which produces a classic 'is the topic there or not?' race in caller retry
 * logic.
 *
 * <p>The bytecode descriptor for the no-options overload is
 * {@code (Ljava/util/Collection;)Lorg/apache/kafka/clients/admin/CreateTopicsResult;}.
 * The options overload is
 * {@code (Ljava/util/Collection;Lorg/apache/kafka/clients/admin/CreateTopicsOptions;)Lorg/apache/kafka/clients/admin/CreateTopicsResult;}.
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/CreateTopicsOptions;}
 * in the descriptor.
 */
public final class BadAdminCreateTopicsNoOptions {

    private static List<NewTopic> topics() {
        return List.of(
                new NewTopic("payments.events", 12, (short) 3),
                new NewTopic("payments.dlq", 3, (short) 3));
    }

    /** Anti-pattern: no CreateTopicsOptions on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public CreateTopicsResult adminNoOptions(Admin admin) {
        return admin.createTopics(topics()); // FIRES — no CreateTopicsOptions
    }

    /** Anti-pattern: no CreateTopicsOptions on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public CreateTopicsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.createTopics(topics()); // FIRES — no CreateTopicsOptions
        }
    }

    /** Control: CreateTopicsOptions passed with explicit long timeout — must NOT fire. */
    public CreateTopicsResult adminWithOptions(Admin admin) {
        CreateTopicsOptions opts = new CreateTopicsOptions().timeoutMs(60_000);
        return admin.createTopics(topics(), opts);
    }
}
