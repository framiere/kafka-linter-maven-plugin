package sample;

import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.common.ConsumerGroupState;

/**
 * RULE: ADMIN_LIST_CONSUMER_GROUPS_NO_OPTIONS.
 *
 * Fires when {@code Admin.listConsumerGroups()} is called without a
 * {@code ListConsumerGroupsOptions} argument. Without {@code inStates(...)}
 * filter, the broker returns EVERY group ever recorded by the coordinator —
 * Stable, PreparingRebalance, CompletingRebalance, Empty, AND Dead — across
 * both the classic and new consumer protocols (KIP-848 in Kafka 3.7+).
 *
 * <p>Failure mode: a Grafana lag dashboard refreshes every 30s by calling
 * {@code admin.listConsumerGroups().all().get()} then iterating to fetch
 * per-group offsets. On a small dev cluster (50 groups) the no-options call
 * takes 200ms and the whole refresh fits in 2s. Promoted to production with
 * 12K consumer groups (mostly Empty/Dead, accumulated over 3 years of
 * dev/staging traffic), the {@code listConsumerGroups} call alone takes 6s
 * and the per-group offset loop takes 4 minutes; the dashboard times out and
 * shows 'no data'. Server-side filter on {@code inStates(STABLE)} drops the
 * live group count to ~240 and refresh completes in 12s.
 *
 * <p>The fanout cost is amplified per-broker because group coordinators are
 * partitioned by group-id hash — every {@code ListGroupsRequest} hits every
 * broker, so the unfiltered cost is N_brokers × per-broker-cost.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code ()Lorg/apache/kafka/clients/admin/ListConsumerGroupsResult;}
 *   <li>With options: {@code (Lorg/apache/kafka/clients/admin/ListConsumerGroupsOptions;)Lorg/apache/kafka/clients/admin/ListConsumerGroupsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/ListConsumerGroupsOptions;}
 * in the descriptor.
 */
public final class BadAdminListConsumerGroupsNoOptions {

    /** Anti-pattern: no ListConsumerGroupsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public ListConsumerGroupsResult adminNoOptions(Admin admin) {
        return admin.listConsumerGroups(); // FIRES — no options
    }

    /** Anti-pattern: no ListConsumerGroupsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public java.util.Collection<ConsumerGroupListing> adminClientNoOptions(Properties props) throws Exception {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.listConsumerGroups().all().get(); // FIRES — no options
        }
    }

    /** Control: ListConsumerGroupsOptions with timeout + inStates(STABLE) — must NOT fire. */
    public ListConsumerGroupsResult adminWithOptions(Admin admin) {
        ListConsumerGroupsOptions opts = new ListConsumerGroupsOptions()
                .timeoutMs(60_000)
                .inStates(Set.of(ConsumerGroupState.STABLE));
        return admin.listConsumerGroups(opts);
    }
}
