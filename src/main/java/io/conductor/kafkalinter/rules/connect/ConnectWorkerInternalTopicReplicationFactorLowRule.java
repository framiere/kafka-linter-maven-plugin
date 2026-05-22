package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires on Kafka Connect WORKER configuration files
 * that declare any of the three internal-topic replication-factor keys
 * EXPLICITLY to a positive integer less than 3:
 *
 * <ul>
 *   <li>{@code config.storage.replication.factor}</li>
 *   <li>{@code offset.storage.replication.factor}</li>
 *   <li>{@code status.storage.replication.factor}</li>
 * </ul>
 *
 * <p>All three default to 3 in the worker's {@code DistributedConfig}
 * ConfigDef. Sub-3 values originate from single-broker dev clusters and
 * leak to production via GitOps / Helm / copy-paste; a single broker
 * outage then takes connector configs, source-connector offsets, or
 * task status offline.
 *
 * <p>Does NOT fire on absent keys (the ConfigDef default applies — safe).
 * Does NOT fire on non-numeric or non-positive values (different failure
 * mode: Connect's {@code AbstractConfig} rejects at startup).
 *
 * <p>Emits one violation per offending key per file (so a worker config
 * with all three keys set to 1 yields three violations).
 */
public final class ConnectWorkerInternalTopicReplicationFactorLowRule implements ProjectScopedRule {

    private static final String CONFIG_STORAGE_RF_KEY = "config.storage.replication.factor";
    private static final String OFFSET_STORAGE_RF_KEY = "offset.storage.replication.factor";
    private static final String STATUS_STORAGE_RF_KEY = "status.storage.replication.factor";
    private static final long MIN_PRODUCTION_RF = 3L;

    private static final String[] KEYS = {
            CONFIG_STORAGE_RF_KEY,
            OFFSET_STORAGE_RF_KEY,
            STATUS_STORAGE_RF_KEY
    };

    private final Severity severity;

    public ConnectWorkerInternalTopicReplicationFactorLowRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_WORKER_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            for (String key : KEYS) {
                Long rf = tryParseLong(trimOrNull(p.getProperty(key)));
                if (rf == null || rf <= 0L || rf >= MIN_PRODUCTION_RF) continue;
                out.add(new Violation(
                        RuleId.CONNECT_WORKER_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW, severity,
                        ctx.relativize(e.getKey()), "key:" + key, 0,
                        "Kafka Connect worker config declares " + key + "=" + rf
                                + " — BELOW the production-recommended replication factor "
                                + "of 3. This key controls the replication factor of one of "
                                + "the worker's THREE durability-critical internal topics: "
                                + "(a) `config.storage.topic` holds every connector's "
                                + "submitted configuration; (b) `offset.storage.topic` holds "
                                + "SOURCE connectors' offset checkpoints (loss causes every "
                                + "source connector to restart from initial position, "
                                + "duplicating every record ever produced); (c) `status."
                                + "storage.topic` holds task/connector status (loss makes "
                                + "the REST API status endpoint return stale data, blinding "
                                + "monitoring). All three default to 3 in DistributedConfig "
                                + "and explicit sub-3 values originate from single-broker "
                                + "dev clusters leaking to production via Helm/GitOps/copy-"
                                + "paste. A single broker outage on the Connect cluster's "
                                + "bootstrap cluster takes the affected topic offline; the "
                                + "damage is asymmetric (zero cost in steady state; "
                                + "catastrophic during broker maintenance). Fix: set "
                                + key + "=3 (or higher) in the worker's connect-distributed."
                                + "properties. NOTE: existing topics retain their original "
                                + "RF — fixing the config is necessary but not sufficient; "
                                + "the operator must also raise the RF of existing topics "
                                + "via kafka-reassign-partitions.sh. If the bootstrap "
                                + "cluster genuinely has < 3 brokers (a dev cluster), "
                                + "suppress this rule explicitly with "
                                + "<CONNECT_WORKER_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW>OFF"
                                + "</CONNECT_WORKER_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW> "
                                + "in the dev-only build profile."));
            }
        }
        return out;
    }

    private static Long tryParseLong(String s) {
        if (s == null) return null;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
