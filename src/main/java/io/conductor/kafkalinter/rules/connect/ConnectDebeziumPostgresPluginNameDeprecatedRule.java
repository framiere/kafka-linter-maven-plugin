package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Project-scoped rule. Fires when a Debezium Postgres connector
 * ({@code connector.class=io.debezium.connector.postgresql.PostgresConnector})
 * is configured with a {@code plugin.name} value that is DEPRECATED in
 * Debezium 2.x and scheduled for REMOVAL in Debezium 3.x.
 *
 * <p>Deprecated values: {@code decoderbufs}, {@code wal2json},
 * {@code wal2json_rds}, {@code wal2json_streaming},
 * {@code wal2json_rds_streaming}. The only forward-compatible value
 * is {@code pgoutput} (Postgres's built-in logical-replication output
 * plugin since Postgres 10).
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumPostgresPluginNameDeprecatedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";
    private static final String PLUGIN_NAME = "plugin.name";

    private static final Set<String> DEPRECATED_PLUGINS = Set.of(
            "decoderbufs",
            "wal2json",
            "wal2json_rds",
            "wal2json_streaming",
            "wal2json_rds_streaming");

    private final Severity severity;

    public ConnectDebeziumPostgresPluginNameDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_POSTGRES_PLUGIN_NAME_DEPRECATED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!POSTGRES_CONNECTOR.equals(connectorClass)) continue;

            String raw = trimOrNull(p.getProperty(PLUGIN_NAME));
            if (raw == null) continue;
            String value = raw.toLowerCase(Locale.ROOT);
            if (!DEPRECATED_PLUGINS.contains(value)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_POSTGRES_PLUGIN_NAME_DEPRECATED, severity,
                    ctx.relativize(e.getKey()), "key:" + PLUGIN_NAME, 0,
                    "Debezium Postgres connector has plugin.name=" + raw + " — this "
                            + "value is DEPRECATED in Debezium 2.x and SCHEDULED FOR "
                            + "REMOVAL in Debezium 3.x. The Debezium 3.x release plan "
                            + "explicitly REMOVES the decoderbufs/wal2json plugin "
                            + "families, leaving pgoutput (Postgres's BUILT-IN "
                            + "logical-replication output plugin since Postgres 10) as "
                            + "the only supported value. In Debezium 2.x the deprecated "
                            + "values still work — the connector starts, the streaming "
                            + "is healthy — but every startup logs a WARN that is "
                            + "rarely noticed in the worker logs. On the day the team "
                            + "upgrades the Connect cluster to Debezium 3.x, every "
                            + "connector with plugin.name=" + raw + " FAILS to start "
                            + "with ConfigException: plugin.name=" + raw + " is no "
                            + "longer supported; the upgrade rolls back; the team "
                            + "scrambles to update connector configs across dozens of "
                            + "environments under deadline pressure. Fix: set "
                            + "plugin.name=pgoutput (or remove the key — pgoutput is "
                            + "the Debezium 2.x default). The migration has NO "
                            + "downstream impact: the emitted Kafka records are "
                            + "IDENTICAL regardless of which Postgres output plugin "
                            + "captured the WAL — Debezium normalizes the output. The "
                            + "only Postgres-side requirement is that the publication "
                            + "exists (which pgoutput requires; Debezium 2.x will "
                            + "auto-create it via publication.autocreate.mode= "
                            + "all_tables by default). If you are mid-migration and "
                            + "coordinating the plugin-switch with a Postgres-side "
                            + "publication-creation, suppress this rule per-fixture "
                            + "with a comment documenting the migration timeline."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
