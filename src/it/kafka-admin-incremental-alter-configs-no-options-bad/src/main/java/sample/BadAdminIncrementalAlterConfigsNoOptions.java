package sample;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

/**
 * RULE: ADMIN_INCREMENTAL_ALTER_CONFIGS_NO_OPTIONS.
 *
 * Fires when
 * {@code Admin.incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>)}
 * is called without an {@code AlterConfigsOptions} argument. The 2-arg form
 * defaults {@code validateOnly=false}, so the broker IMMEDIATELY applies the
 * change with no preview opportunity.
 *
 * <p>Failure mode: a Jenkins job updates topic retention from 7d to 30d for
 * 120 topics flagged by compliance. Dev tests pass — the dev cluster only has
 * 5 topics and all accept the change. In production, one of the 120 happens
 * to be {@code cleanup.policy=compact} which does NOT support time-based
 * retention; the broker returns {@code INVALID_CONFIG} for that one entry,
 * but the OTHER 119 topics have ALREADY been mutated. The change set is now
 * in a half-applied state with no easy undo (previous retention values
 * weren't captured before the call). With {@code validateOnly(true)} first,
 * the dry-run would have surfaced the INVALID_CONFIG error with NOTHING
 * applied.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Map;)Lorg/apache/kafka/clients/admin/AlterConfigsResult;}
 *   <li>With options: {@code (Ljava/util/Map;Lorg/apache/kafka/clients/admin/AlterConfigsOptions;)Lorg/apache/kafka/clients/admin/AlterConfigsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/AlterConfigsOptions;}
 * in the descriptor.
 */
public final class BadAdminIncrementalAlterConfigsNoOptions {

    private static Map<ConfigResource, Collection<AlterConfigOp>> retentionOps() {
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, "payments.events");
        AlterConfigOp set = new AlterConfigOp(
                new ConfigEntry("retention.ms", "2592000000"), OpType.SET);
        return Map.of(topic, List.of(set));
    }

    /** Anti-pattern: no AlterConfigsOptions on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public AlterConfigsResult adminNoOptions(Admin admin) {
        return admin.incrementalAlterConfigs(retentionOps()); // FIRES — no AlterConfigsOptions
    }

    /** Anti-pattern: no AlterConfigsOptions on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public AlterConfigsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.incrementalAlterConfigs(retentionOps()); // FIRES — no AlterConfigsOptions
        }
    }

    /** Control: AlterConfigsOptions with validateOnly(true) dry-run — must NOT fire. */
    public AlterConfigsResult adminDryRun(Admin admin) {
        AlterConfigsOptions opts = new AlterConfigsOptions()
                .timeoutMs(60_000)
                .validateOnly(true);
        return admin.incrementalAlterConfigs(retentionOps(), opts);
    }
}
