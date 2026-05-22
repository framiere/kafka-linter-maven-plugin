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
 * Project-scoped rule. Fires when a Debezium PostgreSQL connector
 * (`io.debezium.connector.postgresql.PostgresConnector`) is configured with
 * `slot.drop.on.stop=true`. Dropping the Postgres replication slot on
 * graceful stop causes silent data loss on every restart cycle: the new
 * connector instance creates a fresh slot at the CURRENT WAL position,
 * skipping all WAL events that elapsed during the stop/restart window.
 */
public final class ConnectDebeziumPostgresSlotDropOnStopTrueRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";
    private static final String SLOT_DROP_ON_STOP = "slot.drop.on.stop";

    private final Severity severity;

    public ConnectDebeziumPostgresSlotDropOnStopTrueRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_POSTGRES_SLOT_DROP_ON_STOP_TRUE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (!POSTGRES_CONNECTOR.equals(connectorClass)) continue;

            String raw = trimOrNull(p.getProperty(SLOT_DROP_ON_STOP));
            if (raw == null) continue;
            if (!"true".equalsIgnoreCase(raw)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_POSTGRES_SLOT_DROP_ON_STOP_TRUE, severity,
                    ctx.relativize(e.getKey()), "key:" + SLOT_DROP_ON_STOP, 0,
                    "Debezium PostgreSQL connector is configured with slot.drop.on.stop=true, "
                            + "which causes Debezium to DROP the Postgres logical-replication "
                            + "slot on graceful connector stop (worker restart, pod migration, "
                            + "DELETE /connectors/<name>, blue-green deploy). After the drop, "
                            + "Postgres garbage-collects the slot and is free to purge the WAL "
                            + "segments that the slot was retaining. When the connector "
                            + "restarts, it creates a FRESH slot at the CURRENT WAL position — "
                            + "silently skipping all change events that occurred between "
                            + "connector-stop and connector-restart. Every routine restart "
                            + "becomes a silent data-loss event. The default value is false "
                            + "(slot persists across stops, WAL is retained, restart resumes "
                            + "seamlessly from confirmed_flush_lsn) and should be used in all "
                            + "production deployments. Remove this setting (or set it to false). "
                            + "The only legitimate use of slot.drop.on.stop=true is in "
                            + "development workflows where a fresh snapshot per restart is "
                            + "desired — not in production."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
