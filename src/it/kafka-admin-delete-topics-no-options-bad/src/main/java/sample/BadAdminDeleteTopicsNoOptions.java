package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;

/**
 * RULE: ADMIN_DELETE_TOPICS_NO_OPTIONS.
 *
 * Fires when {@code Admin.deleteTopics(Collection<String>)} is called without a
 * {@code DeleteTopicsOptions} argument. The no-options overload uses the default
 * {@code request.timeout.ms} (~30s). For delete this is qualitatively worse than
 * the create case: when the client gives up with a TimeoutException, the broker
 * may still be in the middle of asynchronously propagating the topic deletion
 * across the cluster (deletes are not atomic — controller marks the znode as
 * 'marked for deletion', then partitions are removed broker-by-broker). The
 * client cannot tell whether the topic is (a) already gone, (b) about to be
 * gone, or (c) still present because the controller failed to start the delete.
 * Caller retry logic that immediately re-issues {@code deleteTopics} can race
 * with the original deletion finishing — producing a 'topic does not exist'
 * error, or worse, ABA: topic gone → re-created by an unrelated producer →
 * deleted again by the retry, losing the new topic's data.
 *
 * <p>The bytecode descriptor for the no-options overload is
 * {@code (Ljava/util/Collection;)Lorg/apache/kafka/clients/admin/DeleteTopicsResult;}.
 * The options overload is
 * {@code (Ljava/util/Collection;Lorg/apache/kafka/clients/admin/DeleteTopicsOptions;)Lorg/apache/kafka/clients/admin/DeleteTopicsResult;}.
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/DeleteTopicsOptions;}
 * in the descriptor.
 */
public final class BadAdminDeleteTopicsNoOptions {

    private static List<String> topics() {
        return List.of("payments.events.v1", "payments.dlq.v1");
    }

    /** Anti-pattern: no DeleteTopicsOptions on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DeleteTopicsResult adminNoOptions(Admin admin) {
        return admin.deleteTopics(topics()); // FIRES — no DeleteTopicsOptions
    }

    /** Anti-pattern: no DeleteTopicsOptions on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DeleteTopicsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.deleteTopics(topics()); // FIRES — no DeleteTopicsOptions
        }
    }

    /** Control: DeleteTopicsOptions passed with explicit long timeout — must NOT fire. */
    public DeleteTopicsResult adminWithOptions(Admin admin) {
        DeleteTopicsOptions opts = new DeleteTopicsOptions().timeoutMs(60_000);
        return admin.deleteTopics(topics(), opts);
    }
}
