package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

/**
 * RULE: ADMIN_ALTER_CONFIGS_DEPRECATED.
 *
 * <p>This fixture exercises the three shapes the rule must catch on the
 * {@link Admin} / {@link org.apache.kafka.clients.admin.AdminClient} type:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on the no-options overload
 *       {@code alterConfigs(Map<ConfigResource, Config>)} — the single
 *       most-common appearance in legacy code.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on the with-options overload
 *       {@code alterConfigs(Map<ConfigResource, Config>, AlterConfigsOptions)}
 *       — same deprecation status; the options arg does not change the
 *       hazard (it adds timeout/validate-only knobs but the full-replacement
 *       semantics are unchanged).</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code admin::alterConfigs} stored into a
 *       {@code BiFunction<Map, AlterConfigsOptions, AlterConfigsResult>} —
 *       the user-class bytecode contains zero {@code INVOKE*} targeting
 *       {@code alterConfigs}; the call lives inside a LambdaMetafactory-
 *       synthesized bridge whose {@code REF_invokeInterface} handle the
 *       rule's INVOKEDYNAMIC walk recognises.</li>
 * </ol>
 *
 * <h2>Why this method family is data-destructive</h2>
 *
 * <p>{@code alterConfigs(Map<ConfigResource, Config>)} sends an
 * {@code AlterConfigsRequest} to the broker carrying the COMPLETE desired
 * state for each resource. The broker overwrites the resource's metadata
 * entry with that exact content; any key NOT present in the supplied
 * {@link Config} is reset to the broker's default value. A call meaning
 * "raise retention on this topic to 7 days" — issued by passing a Config
 * with just the single {@code retention.ms} entry — ALSO wipes that topic's
 * {@code cleanup.policy}, {@code compression.type},
 * {@code min.insync.replicas}, and every other previously-set per-topic
 * config back to broker defaults. For a compacted topic this is
 * irreversible: {@code cleanup.policy} flips from {@code compact} to the
 * broker default {@code delete}, and the log cleaner starts segment-deleting
 * compacted records on the next run. By the time anyone notices, weeks of
 * compacted state may be gone.
 *
 * <p>The KIP-339 replacement
 * {@code incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>)}
 * is shown in the {@code -good} fixture.
 *
 * <h2>Why we still consume the futures here</h2>
 *
 * <p>Each call returns an {@link AlterConfigsResult}; discarding it would
 * trip {@code ADMIN_RESULT_DISCARDED} (default ERROR) and pollute the
 * fixture with unrelated violations. We call {@code .all().get()} on every
 * result to satisfy that sibling rule, isolating the failure cause to
 * {@code ADMIN_ALTER_CONFIGS_DEPRECATED} specifically.
 */
public final class BadAdminAlterConfigsDeprecated {

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "bad-alter-configs-deprecated");
        return p;
    }

    public void raiseRetentionViaDeprecatedNoOptions(String topic) throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(adminProps())) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config singleKey = new Config(List.of(new ConfigEntry("retention.ms", "604800000")));
            // FIRES — direct INVOKEINTERFACE on the deprecated no-options overload.
            // This silently wipes cleanup.policy, compression.type, min.insync.replicas, ... back to broker defaults.
            AlterConfigsResult result = admin.alterConfigs(Map.of(resource, singleKey));
            result.all().get();  // consume to silence ADMIN_RESULT_DISCARDED
            admin.close(Duration.ofSeconds(30));
        }
    }

    public void raiseRetentionViaDeprecatedWithOptions(String topic) throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(adminProps())) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config singleKey = new Config(List.of(new ConfigEntry("retention.ms", "604800000")));
            AlterConfigsOptions opts = new AlterConfigsOptions().timeoutMs(30_000).validateOnly(false);
            // FIRES — direct INVOKEINTERFACE on the deprecated with-options overload.
            // The options control transport behaviour; the full-replacement hazard is unchanged.
            AlterConfigsResult result = admin.alterConfigs(Map.of(resource, singleKey), opts);
            result.all().get();
            admin.close(Duration.ofSeconds(30));
        }
    }

    public void captureAlterConfigsAsMethodReference(String topic) throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(adminProps())) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config singleKey = new Config(List.of(new ConfigEntry("retention.ms", "604800000")));
            AlterConfigsOptions opts = new AlterConfigsOptions().timeoutMs(30_000);
            // FIRES — INVOKEDYNAMIC method-ref capture. The user-class bytecode contains
            // ZERO INVOKE* instructions targeting alterConfigs; the rule's INVOKEDYNAMIC
            // walk inspects bsmArgs handles and detects the REF_invokeInterface capture.
            BiFunction<Map<ConfigResource, Config>, AlterConfigsOptions, AlterConfigsResult> deferred =
                    admin::alterConfigs;
            AlterConfigsResult result = deferred.apply(Map.of(resource, singleKey), opts);
            result.all().get();
            admin.close(Duration.ofSeconds(30));
        }
    }
}
