package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Silent: this fixture only calls
 * {@code incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>)},
 * the KIP-339 replacement for the deprecated {@code alterConfigs(...)} family.
 * The rule matches by EXACT method name {@code alterConfigs} (not a prefix),
 * so {@code incrementalAlterConfigs} — a different name — must NOT be flagged.
 * This fixture pins that boolean: zero ADMIN_ALTER_CONFIGS_DEPRECATED violations.
 *
 * <h2>Why incrementalAlterConfigs is the safe replacement</h2>
 *
 * <p>{@code incrementalAlterConfigs} accepts a per-key
 * {@link AlterConfigOp} that names the operation type:
 * <ul>
 *   <li>{@code SET} — overwrite this key with the supplied value.</li>
 *   <li>{@code DELETE} — remove this key (reset to broker default).</li>
 *   <li>{@code APPEND} — for list-valued keys, add to the list.</li>
 *   <li>{@code SUBTRACT} — for list-valued keys, remove from the list.</li>
 * </ul>
 * Each operation applies SURGICALLY to the named key only. Keys not mentioned
 * in the request are left untouched. The "raise retention on one topic"
 * intent translates cleanly to a single SET operation on {@code retention.ms};
 * every other per-topic config (cleanup.policy, compression.type,
 * min.insync.replicas, ...) keeps its previous value.
 *
 * <p>The request is transported as {@code IncrementalAlterConfigsRequest} v0+,
 * routed to the controller broker (which owns the topic-config write path),
 * and applied atomically per-resource. There is no read-then-write window —
 * concurrent admin edits to OTHER keys on the same resource are preserved.
 *
 * <h2>Sibling: passing IncrementalAlterConfigsOptions</h2>
 *
 * <p>The with-options overload
 * {@code incrementalAlterConfigs(configs, options)} is the production-grade
 * shape: pin a {@code timeoutMs} so the call fails fast on an unhealthy
 * controller, and pin {@code validateOnly(false)} to make the dry-run
 * behaviour explicit. The no-options overload exists for tests and would
 * trip {@code ADMIN_INCREMENTAL_ALTER_CONFIGS_NO_OPTIONS} (turned OFF in
 * this fixture's pom) — that's a separate rule, not in scope here.
 */
public final class GoodAdminIncrementalAlterConfigs {

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "good-incremental-alter-configs");
        return p;
    }

    public void raiseRetentionViaIncremental(String topic) throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(adminProps())) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            AlterConfigOp op = new AlterConfigOp(
                    new ConfigEntry("retention.ms", "604800000"),
                    AlterConfigOp.OpType.SET);
            // SILENT — incrementalAlterConfigs is a different method name and is NOT
            // flagged by ADMIN_ALTER_CONFIGS_DEPRECATED. Only the named key is mutated;
            // cleanup.policy, compression.type, min.insync.replicas, etc. are preserved.
            AlterConfigsResult result = admin.incrementalAlterConfigs(Map.of(resource, List.of(op)));
            result.all().get();
            admin.close(Duration.ofSeconds(30));
        }
    }
}
