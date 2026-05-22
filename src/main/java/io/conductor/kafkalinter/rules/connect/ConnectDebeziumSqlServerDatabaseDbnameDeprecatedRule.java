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
 * Project-scoped rule. Fires when a Debezium SQL Server connector
 * ({@code connector.class=io.debezium.connector.sqlserver.SqlServerConnector})
 * has the legacy {@code database.dbname} key set.
 *
 * <p>This key was the correct way to specify the captured database in
 * Debezium 1.x, but was DEPRECATED in Debezium 2.0 (in favor of
 * {@code database.names=<csv>}, supporting multi-database capture) and
 * REMOVED entirely in Debezium 2.5. In 2.5+, Connect silently accepts
 * the unknown key but the connector emits no records.
 *
 * <p>Rule is scoped strictly to the SQL Server connector class: on
 * Postgres/Oracle/DB2 connectors, {@code database.dbname} is the
 * correct, non-deprecated key.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumSqlServerDatabaseDbnameDeprecatedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String SQLSERVER_CONNECTOR = "io.debezium.connector.sqlserver.SqlServerConnector";
    private static final String DATABASE_DBNAME = "database.dbname";

    private final Severity severity;

    public ConnectDebeziumSqlServerDatabaseDbnameDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_SQL_SERVER_DATABASE_DBNAME_DEPRECATED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!SQLSERVER_CONNECTOR.equals(connectorClass)) continue;

            String dbname = trimOrNull(p.getProperty(DATABASE_DBNAME));
            if (dbname == null) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_SQL_SERVER_DATABASE_DBNAME_DEPRECATED, severity,
                    ctx.relativize(e.getKey()), "key:" + DATABASE_DBNAME, 0,
                    "Debezium SQL Server connector has database.dbname=" + dbname + " — "
                            + "this key was the correct way to specify the captured "
                            + "database in Debezium 1.x, but was DEPRECATED in "
                            + "Debezium 2.0 (in favor of database.names=<csv>, "
                            + "supporting multi-database capture from a single "
                            + "connector) and REMOVED entirely from the connector's "
                            + "ConfigDef in Debezium 2.5. In Debezium 2.5+, Connect "
                            + "silently accepts the unknown key (Connect's general "
                            + "unknown-key behavior is accept-and-ignore — the "
                            + "REST-API PUT-validate endpoint does NOT flag the "
                            + "deprecated key), but the connector's required "
                            + "database.names config is empty: the connector either "
                            + "throws IllegalArgumentException: No databases "
                            + "configured for capture at start(), or runs cleanly "
                            + "with an empty capture loop emitting ZERO records. The "
                            + "connector's REST API reports RUNNING; the CDC topics "
                            + "are empty; the typical diagnosis time is 1-2 days "
                            + "before someone notices the rename. Fix: migrate the "
                            + "config by replacing database.dbname=" + dbname + " with "
                            + "database.names=" + dbname + " (single-value CSV — "
                            + "Debezium 2.x accepts a single name or a comma-separated "
                            + "list). For multi-database capture (a 2.x-only feature), "
                            + "provide a CSV: database.names=db1,db2,db3. The "
                            + "topic.prefix config is unchanged (single value, used "
                            + "as the namespace prefix for all captured databases' "
                            + "topic names). The migration has NO downstream impact "
                            + "(the emitted Kafka records' schema is identical) and "
                            + "no SQL-Server-side changes (the CDC tables and Agent "
                            + "capture jobs are unaffected). Note: this rule fires "
                            + "ONLY on SQL Server connectors. On Postgres, Oracle, "
                            + "and DB2 Debezium connectors, database.dbname remains "
                            + "the correct, non-deprecated key — those connectors "
                            + "did not rename their database-selection config."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
