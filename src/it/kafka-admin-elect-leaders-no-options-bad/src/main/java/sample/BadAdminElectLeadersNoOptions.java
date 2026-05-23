package sample;

import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: ADMIN_ELECT_LEADERS_NO_OPTIONS.
 *
 * Fires when {@code Admin.electLeaders(ElectionType, Set<TopicPartition>)}
 * is called without an {@code ElectLeadersOptions} argument. Leader
 * election is a controller-driven operation:
 *
 * <ul>
 *   <li>{@code PREFERRED} — the broker at AR position 0 becomes leader if
 *       it is in the ISR. Typically sub-second on a healthy cluster, but
 *       queues behind other admin writes on contended controllers and can
 *       easily exceed the default ~30 s timeout during rolling restarts or
 *       under concurrent Cruise Control moves.</li>
 *   <li>{@code UNCLEAN} — allows electing an out-of-sync replica when no
 *       in-sync replica is available, accepting data-loss risk. The newly
 *       elected replica must rebuild its log state from scratch (often
 *       re-fetching from peers), which takes MINUTES per partition. This
 *       is a last-resort, data-loss-accepting operation invoked under
 *       incident pressure — the WORST possible moment for a confusing
 *       "did it succeed or not?" timeout.</li>
 * </ul>
 *
 * <p>Failure mode (preferred-leader rebalance cron reports phantom
 * failure):
 * <ol>
 *   <li>Nightly cron:
 *       {@code admin.electLeaders(PREFERRED, partitionsToRebalance).all().get()}
 *       (no options).</li>
 *   <li>Staging completes in 4 s; production with 12K partitions has the
 *       controller concurrently processing Cruise Control reassignments;
 *       the metadata log lags ~5 s; election queues behind that.</li>
 *   <li>Default 30 s timeout fires; the cron logs
 *       'FAILURE: leader rebalance failed' and pages on-call.</li>
 *   <li>On-call investigates for 20 minutes and discovers via
 *       {@code kafka-topics --describe} that leadership IS rebalanced —
 *       the controller succeeded async; the future just gave up early.
 *       The wasted page is the surface symptom; loss of trust in the
 *       rebalance tooling is the deeper cost.</li>
 * </ol>
 *
 * <p>Fix: {@code new ElectLeadersOptions().timeoutMs(120_000)} for
 * preferred elections, {@code .timeoutMs(300_000)} for unclean elections.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Lorg/apache/kafka/common/ElectionType;Ljava/util/Set;)Lorg/apache/kafka/clients/admin/ElectLeadersResult;}
 *   <li>With options: {@code (Lorg/apache/kafka/common/ElectionType;Ljava/util/Set;Lorg/apache/kafka/clients/admin/ElectLeadersOptions;)Lorg/apache/kafka/clients/admin/ElectLeadersResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/ElectLeadersOptions;}
 * in the descriptor.
 */
public final class BadAdminElectLeadersNoOptions {

    private static Set<TopicPartition> partitions() {
        return Set.of(
                new TopicPartition("payments.events", 0),
                new TopicPartition("payments.events", 1),
                new TopicPartition("payments.events", 2));
    }

    /** Anti-pattern: no ElectLeadersOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public ElectLeadersResult adminNoOptions(Admin admin) {
        return admin.electLeaders(ElectionType.PREFERRED, partitions()); // FIRES — no options
    }

    /** Anti-pattern: no ElectLeadersOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public ElectLeadersResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.electLeaders(ElectionType.PREFERRED, partitions()); // FIRES — no options
        }
    }

    /** Control: ElectLeadersOptions with generous timeout — must NOT fire. */
    public ElectLeadersResult adminWithOptions(Admin admin) {
        ElectLeadersOptions opts = new ElectLeadersOptions().timeoutMs(120_000);
        return admin.electLeaders(ElectionType.PREFERRED, partitions(), opts);
    }
}
