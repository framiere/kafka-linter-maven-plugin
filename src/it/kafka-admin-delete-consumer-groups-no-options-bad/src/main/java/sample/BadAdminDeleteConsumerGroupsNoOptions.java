package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;

/**
 * RULE: ADMIN_DELETE_CONSUMER_GROUPS_NO_OPTIONS.
 *
 * Fires when {@code Admin.deleteConsumerGroups(Collection<String>)} is called
 * without a {@code DeleteConsumerGroupsOptions} argument. The deletion is
 * IRREVERSIBLE — once a group is deleted, its committed-offset history is
 * tombstoned in {@code __consumer_offsets} and there is no 'undelete'. The
 * next consumer using the group-id starts from {@code auto.offset.reset}
 * with no memory of prior progress.
 *
 * <p>Failure mode: an environment-cleanup script deletes all consumer groups
 * matching {@code dev-*} prefix at end-of-day. The script calls
 * {@code admin.deleteConsumerGroups(devGroups).all().get()} where
 * {@code devGroups} has 200 entries. The default ~30 s timeout fires;
 * 137 of 200 are deleted; the catch block logs 'cleanup partial failure'.
 * The next day, the leftover 63 still appear in the lag dashboard (zero
 * members, no committed offsets), confusing developers. After three days
 * the cluster has 500 zombie groups. With a 120 s timeout AND per-group
 * future inspection via {@code result.deletedGroups().get(groupId).get()},
 * the caller knows which deletions succeeded and which timed out.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Collection;)Lorg/apache/kafka/clients/admin/DeleteConsumerGroupsResult;}
 *   <li>With options: {@code (Ljava/util/Collection;Lorg/apache/kafka/clients/admin/DeleteConsumerGroupsOptions;)Lorg/apache/kafka/clients/admin/DeleteConsumerGroupsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/DeleteConsumerGroupsOptions;}
 * in the descriptor.
 */
public final class BadAdminDeleteConsumerGroupsNoOptions {

    private static List<String> groups() {
        return List.of("dev-aggregator-1", "dev-aggregator-2", "dev-scratch-3");
    }

    /** Anti-pattern: no DeleteConsumerGroupsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public DeleteConsumerGroupsResult adminNoOptions(Admin admin) {
        return admin.deleteConsumerGroups(groups()); // FIRES — no options
    }

    /** Anti-pattern: no DeleteConsumerGroupsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public DeleteConsumerGroupsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.deleteConsumerGroups(groups()); // FIRES — no options
        }
    }

    /** Control: DeleteConsumerGroupsOptions with generous timeout — must NOT fire. */
    public DeleteConsumerGroupsResult adminWithOptions(Admin admin) {
        DeleteConsumerGroupsOptions opts = new DeleteConsumerGroupsOptions()
                .timeoutMs(120_000);
        return admin.deleteConsumerGroups(groups(), opts);
    }
}
