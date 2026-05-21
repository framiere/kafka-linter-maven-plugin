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
 * Project-scoped rule. Fires when a Debezium connector .properties file
 * declares BOTH {@code <scope>.include.list} AND {@code <scope>.exclude.list}
 * for any of the five Debezium filter scopes:
 * <ul>
 *   <li>{@code table} — table-level filtering (Postgres/MySQL/SQL Server/Oracle)</li>
 *   <li>{@code database} — database-level filtering (MySQL multi-DB, SQL Server multi-DB)</li>
 *   <li>{@code schema} — Postgres schema filtering</li>
 *   <li>{@code collection} — MongoDB collection filtering</li>
 *   <li>{@code column} — column-level filtering (PII redaction, bandwidth)</li>
 * </ul>
 *
 * <p>Debezium's documented behavior: when both lists are set for the
 * same scope, the include list wins and the exclude list is SILENTLY
 * DROPPED (a single WARN log line at connector start). The operator's
 * exclude rules — often written for PII compliance — have NO EFFECT.
 *
 * <p>Detection: connector.class starts with {@code io.debezium.connector.};
 * for each of the five scopes, if both include and exclude are present
 * and non-empty (after trim), emit one ERROR-severity violation per scope.
 */
public final class ConnectDebeziumIncludeAndExcludeListBothSetRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_PREFIX = "io.debezium.connector.";
    private static final String[] SCOPES = {"table", "database", "schema", "collection", "column"};

    private final Severity severity;

    public ConnectDebeziumIncludeAndExcludeListBothSetRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_INCLUDE_AND_EXCLUDE_LIST_BOTH_SET;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !connectorClass.startsWith(DEBEZIUM_PREFIX)) continue;

            for (String scope : SCOPES) {
                String includeKey = scope + ".include.list";
                String excludeKey = scope + ".exclude.list";
                String includeValue = trimOrNull(p.getProperty(includeKey));
                String excludeValue = trimOrNull(p.getProperty(excludeKey));
                if (includeValue == null || excludeValue == null) continue;

                out.add(new Violation(
                        RuleId.CONNECT_DEBEZIUM_INCLUDE_AND_EXCLUDE_LIST_BOTH_SET, severity,
                        ctx.relativize(e.getKey()), "key:" + excludeKey, 0,
                        "Debezium connector " + connectorClass + " declares BOTH "
                                + includeKey + "=" + includeValue + " AND "
                                + excludeKey + "=" + excludeValue + " — these are MUTUALLY "
                                + "EXCLUSIVE per Debezium's documented behavior. The runtime "
                                + "will use ONLY the include list and SILENTLY DROP the "
                                + "exclude list (a single WARN log line at connector start, "
                                + "rarely caught in log aggregation). The exclude rules "
                                + "(values: " + excludeValue + ") have NO EFFECT on the actual "
                                + "captured change stream. This is the #1 cause of 'sensitive "
                                + "column still in the Kafka topic' compliance findings: "
                                + "operator writes an exclude.list to drop a PII column or "
                                + "internal table, but an existing include.list silently "
                                + "preempts it. Delete ONE of the two lists for the '" + scope
                                + "' scope: keep " + includeKey + " (narrow allow-list) OR "
                                + "keep " + excludeKey + " (broad block-list), never both."));
            }
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
