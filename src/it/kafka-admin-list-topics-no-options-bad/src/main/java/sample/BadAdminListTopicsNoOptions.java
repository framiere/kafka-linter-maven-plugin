package sample;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;

/**
 * RULE: ADMIN_LIST_TOPICS_NO_OPTIONS.
 *
 * Fires when {@code Admin.listTopics()} is called without a
 * {@code ListTopicsOptions} argument. Two silent defaults bite callers:
 *
 * <ol>
 *   <li><b>Timeout</b>: inherits {@code default.api.timeout.ms} (~30s).
 *   <li><b>{@code listInternal}</b>: defaults to {@code false}, so the result
 *       EXCLUDES {@code __consumer_offsets}, {@code __transaction_state},
 *       every Streams {@code *-changelog}/{@code *-repartition} topic, and
 *       every Confluent internal topic.
 * </ol>
 *
 * <p>The killer scenario: a backup script iterates
 * {@code admin.listTopics().names().get()} and snapshots each topic. With 200
 * user topics and 50 internal topics, the script backs up 200 and reports
 * success. On disaster-recovery day, the restored cluster has user data but
 * NO consumer-offset history — every consumer group restarts from
 * {@code auto.offset.reset}, losing in-flight progress.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code ()Lorg/apache/kafka/clients/admin/ListTopicsResult;}
 *   <li>With options: {@code (Lorg/apache/kafka/clients/admin/ListTopicsOptions;)Lorg/apache/kafka/clients/admin/ListTopicsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/ListTopicsOptions;}
 * in the descriptor.
 */
public final class BadAdminListTopicsNoOptions {

    /** Anti-pattern: no ListTopicsOptions on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public ListTopicsResult adminNoOptions(Admin admin) {
        return admin.listTopics(); // FIRES — no ListTopicsOptions
    }

    /** Anti-pattern: no ListTopicsOptions on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public Set<String> adminClientNoOptions(Properties props) throws InterruptedException, ExecutionException {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.listTopics().names().get(); // FIRES — no ListTopicsOptions
        }
    }

    /** Control: ListTopicsOptions with explicit timeout AND listInternal(true) — must NOT fire. */
    public ListTopicsResult adminWithOptions(Admin admin) {
        ListTopicsOptions opts = new ListTopicsOptions()
                .timeoutMs(60_000)
                .listInternal(true);
        return admin.listTopics(opts);
    }
}
