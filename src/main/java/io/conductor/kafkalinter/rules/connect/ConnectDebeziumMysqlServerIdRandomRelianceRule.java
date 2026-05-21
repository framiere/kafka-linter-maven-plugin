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
 * Project-scoped rule. Fires when a Debezium MySQL connector omits
 * (or empty-values) {@code database.server.id}.
 *
 * <p>Without an explicit id, Debezium picks a random integer in
 * [5400, 6400] at every task startup. The 1000-wide window has a
 * birthday-paradox collision probability of ~50% at 37 connectors;
 * the per-restart re-randomization also makes audit-log queries and
 * DBA observability impossible.
 *
 * <p>Complementary to
 * {@link ConnectDebeziumMysqlServerIdSharedRule} which catches
 * explicit-id collisions.
 */
public final class ConnectDebeziumMysqlServerIdRandomRelianceRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MYSQL_CONNECTOR = "io.debezium.connector.mysql.MySqlConnector";
    private static final String SERVER_ID = "database.server.id";

    private final Severity severity;

    public ConnectDebeziumMysqlServerIdRandomRelianceRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_MYSQL_SERVER_ID_RANDOM_RELIANCE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!MYSQL_CONNECTOR.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            if (trimOrNull(p.getProperty(SERVER_ID)) != null) continue;

            String connectorName = trimOrNull(p.getProperty("name"));
            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_MYSQL_SERVER_ID_RANDOM_RELIANCE, severity,
                    ctx.relativize(e.getKey()), "key:" + SERVER_ID, 0,
                    "Debezium MySQL connector "
                            + (connectorName != null ? "(name=" + connectorName + ") " : "")
                            + "does not declare database.server.id. Debezium will fall back "
                            + "to a RANDOM integer in [5400, 6400] at every task startup. "
                            + "This 1000-wide window has a birthday-collision probability "
                            + "of ~50% at 37 connectors and >99% at 100 connectors; "
                            + "additionally, the id is re-randomized on every restart "
                            + "(worker rebalance, config change, pod migration), so the "
                            + "connector's identity DRIFTS — MySQL audit-log queries by "
                            + "server_id return nothing, SHOW REPLICA HOSTS accumulates "
                            + "ghost entries, server-side rate-limits keyed by server_id "
                            + "are useless, and any colliding MySQL replica gets kicked "
                            + "off intermittently. Set database.server.id=<int> explicitly, "
                            + "using a value from your org's server-id allocation registry."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
