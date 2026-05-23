package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;

/**
 * RULE: ADMIN_DESCRIBE_CONFIGS_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeConfigs(Collection<ConfigResource>)} is
 * called without a {@code DescribeConfigsOptions} argument. Three silent
 * defaults bite callers:
 *
 * <ol>
 *   <li><b>Timeout</b>: inherits {@code request.timeout.ms} (~30s).
 *   <li><b>{@code includeSynonyms}</b>: defaults to {@code false}, so each
 *       {@code ConfigEntry.synonyms()} is empty — the caller cannot see
 *       whether {@code retention.ms=604800000} came from a topic-override,
 *       the broker default, or the static broker config.
 *   <li><b>{@code includeDocumentation}</b>: defaults to {@code false}, so
 *       each {@code ConfigEntry.documentation()} is {@code null} — UI tools
 *       cannot show 'what does this key do?' next to the value.
 * </ol>
 *
 * <p>Failure mode: a migration script reads topic configs via
 * {@code admin.describeConfigs(List.of(topicResource))} and copies every
 * entry to the new cluster via {@code incrementalAlterConfigs}. Without
 * {@code includeSynonyms}, the script cannot distinguish topic-overrides
 * from inherited broker-defaults; it copies BOTH, materializing the broker
 * defaults as explicit topic-overrides on the new cluster. Any future
 * broker-wide {@code retention.ms} change won't apply to migrated topics.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Collection;)Lorg/apache/kafka/clients/admin/DescribeConfigsResult;}
 *   <li>With options: {@code (Ljava/util/Collection;Lorg/apache/kafka/clients/admin/DescribeConfigsOptions;)Lorg/apache/kafka/clients/admin/DescribeConfigsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/DescribeConfigsOptions;}
 * in the descriptor.
 */
public final class BadAdminDescribeConfigsNoOptions {

    private static List<ConfigResource> topicResources() {
        return List.of(
                new ConfigResource(ConfigResource.Type.TOPIC, "payments.events"),
                new ConfigResource(ConfigResource.Type.TOPIC, "payments.dlq"));
    }

    /** Anti-pattern: no DescribeConfigsOptions on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DescribeConfigsResult adminNoOptions(Admin admin) {
        return admin.describeConfigs(topicResources()); // FIRES — no DescribeConfigsOptions
    }

    /** Anti-pattern: no DescribeConfigsOptions on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeConfigsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeConfigs(topicResources()); // FIRES — no DescribeConfigsOptions
        }
    }

    /** Control: DescribeConfigsOptions with timeout + synonyms + documentation — must NOT fire. */
    public DescribeConfigsResult adminWithOptions(Admin admin) {
        DescribeConfigsOptions opts = new DescribeConfigsOptions()
                .timeoutMs(60_000)
                .includeSynonyms(true)
                .includeDocumentation(true);
        return admin.describeConfigs(topicResources(), opts);
    }
}
