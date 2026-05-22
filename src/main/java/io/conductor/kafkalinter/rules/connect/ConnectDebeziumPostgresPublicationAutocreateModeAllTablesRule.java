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

/**
 * Project-scoped rule. Fires when a Debezium PostgreSQL connector
 * ({@code io.debezium.connector.postgresql.PostgresConnector}) is
 * explicitly configured with {@code publication.autocreate.mode=all_tables}.
 *
 * <p>The {@code all_tables} mode tells Debezium to execute
 * {@code CREATE PUBLICATION <name> FOR ALL TABLES;} which captures EVERY
 * table in the database (including PII tables, tables added later, and
 * tables outside the operator's intended scope). The downstream filter
 * ({@code table.include.list}) does NOT prevent capture — every captured
 * table's WAL flows through the slot and the connector process, and is
 * only filtered AFTER exposure.
 *
 * <p>The rule fires only on the explicit value {@code all_tables}; the
 * absent case is left silent (defaults vary by Debezium version).
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumPostgresPublicationAutocreateModeAllTablesRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String POSTGRES_CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector";
    private static final String PUBLICATION_AUTOCREATE_MODE = "publication.autocreate.mode";

    private final Severity severity;

    public ConnectDebeziumPostgresPublicationAutocreateModeAllTablesRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_POSTGRES_PUBLICATION_AUTOCREATE_MODE_ALL_TABLES;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (!POSTGRES_CONNECTOR_CLASS.equals(connectorClass)) continue;

            String mode = trimOrNull(p.getProperty(PUBLICATION_AUTOCREATE_MODE));
            if (mode == null) continue;
            if (!"all_tables".equals(mode.toLowerCase(Locale.ROOT))) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_POSTGRES_PUBLICATION_AUTOCREATE_MODE_ALL_TABLES, severity,
                    ctx.relativize(e.getKey()), "key:" + PUBLICATION_AUTOCREATE_MODE, 0,
                    "Debezium PostgreSQL connector has publication.autocreate.mode=" + mode
                            + " explicitly. This tells Debezium to execute `CREATE PUBLICATION "
                            + "<publication.name> FOR ALL TABLES;` against Postgres at startup — "
                            + "creating a publication that captures EVERY table in the database, "
                            + "including: tables outside the operator's intended scope (audit "
                            + "tables, secrets-storage tables, materialized views' base tables); "
                            + "tables added later by other teams (a new `users_pii` table next "
                            + "quarter is silently captured); in some configurations, Postgres "
                            + "system catalog tables. The connector's downstream filter "
                            + "(table.include.list etc.) does NOT prevent capture — every captured "
                            + "table's WAL flows through the slot, is decoded by the connector "
                            + "process, sits in the connector's memory and on the database.history "
                            + "Kafka topic, and is only filtered AFTER the data has already been "
                            + "exposed to the connector. The `all_tables` mode also requires the "
                            + "connector's Postgres role to be SUPERUSER or hold pg_create_"
                            + "subscription (Postgres 16+) — a permission elevation most "
                            + "production-DBA-approved roles do not grant. The Debezium 2.x+ "
                            + "default was changed FROM all_tables TO filtered SPECIFICALLY because "
                            + "the community recognized all_tables as a security, data-governance, "
                            + "and operational-blast-radius footgun. Fix: switch to "
                            + "publication.autocreate.mode=filtered and provide a filter-list "
                            + "(typically table.include.list=<schema>.<table>,...) — this generates "
                            + "`CREATE PUBLICATION <name> FOR TABLE <derived-list>;` which captures "
                            + "ONLY the named tables and refuses to capture tables added later."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
