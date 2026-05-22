package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Project-scoped rule. Fires when a Confluent JDBC source connector
 * ({@code io.confluent.connect.jdbc.JdbcSourceConnector}) is configured
 * with {@code query=<SELECT ...>} AND at least one of the mutually-
 * exclusive table-mode configs ({@code table.whitelist},
 * {@code table.blacklist}, {@code catalog.pattern}, {@code schema.pattern}).
 *
 * <p>The Confluent connector rejects this combination with a
 * {@code ConfigException} at validate() time. The rule catches the
 * conflict at PR time.
 *
 * <p>Emits one violation per offending file (listing all conflicting keys).
 */
public final class ConnectJdbcSourceQueryAndTableBothSetRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String JDBC_SOURCE_CLASS = "io.confluent.connect.jdbc.JdbcSourceConnector";
    private static final String QUERY = "query";
    private static final String[] TABLE_MODE_KEYS = {
            "table.whitelist",
            "table.blacklist",
            "catalog.pattern",
            "schema.pattern"
    };

    private final Severity severity;

    public ConnectJdbcSourceQueryAndTableBothSetRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_JDBC_SOURCE_QUERY_AND_TABLE_BOTH_SET;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (!JDBC_SOURCE_CLASS.equals(connectorClass)) continue;

            String query = trimOrNull(p.getProperty(QUERY));
            if (query == null) continue;

            Set<String> conflicts = new LinkedHashSet<>();
            for (String tableModeKey : TABLE_MODE_KEYS) {
                if (trimOrNull(p.getProperty(tableModeKey)) != null) {
                    conflicts.add(tableModeKey);
                }
            }
            if (conflicts.isEmpty()) continue;

            out.add(new Violation(
                    RuleId.CONNECT_JDBC_SOURCE_QUERY_AND_TABLE_BOTH_SET, severity,
                    ctx.relativize(e.getKey()), "key:" + QUERY, 0,
                    "Confluent JDBC source connector has `query` set AND mutually-exclusive "
                            + "table-mode config(s) set: " + String.join(", ", conflicts)
                            + ". The `query` config and the table-mode configs are two completely "
                            + "different ways of specifying what to poll: `query` provides a custom "
                            + "SQL SELECT (emitted to a SINGLE topic, no table discovery); the table-"
                            + "mode configs (table.whitelist, table.blacklist, catalog.pattern, "
                            + "schema.pattern) drive table discovery from the database catalog (one "
                            + "topic per table). Setting both is logically inconsistent — the "
                            + "connector cannot both 'use this hand-written query' AND 'iterate "
                            + "discovered tables'. The connector's validate() rejects with "
                            + "`ConfigException: Cannot specify both 'query' and 'table.whitelist'"
                            + "/'table.blacklist'/'catalog.pattern'/'schema.pattern' in the same "
                            + "connector configuration` — the connector REFUSES TO START. Fix: "
                            + "delete the conflicting key(s). Keep `query=<SELECT ...>` if a custom "
                            + "JOIN/projection/filter is needed (remove all table-mode keys); OR "
                            + "remove `query` and keep the table-mode keys if table discovery is "
                            + "what the operator actually wants."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
