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
 * Project-scoped rule. Fires when a Debezium MySQL connector
 * ({@code connector.class=io.debezium.connector.mysql.MySqlConnector})
 * is configured with {@code include.query=true}.
 *
 * <p>With {@code include.query=true}, the connector embeds the RAW SQL
 * STATEMENT that produced each change into the emitted Debezium
 * envelope's {@code source.query} field — full statement, full parameter
 * values inlined, comments included. Every downstream consumer of the
 * CDC topic then has read access to potentially-sensitive SQL text
 * (PII, passwords, payment cards, proprietary application internals).
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumMysqlIncludeQueryTrueRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MYSQL_CONNECTOR = "io.debezium.connector.mysql.MySqlConnector";
    private static final String INCLUDE_QUERY = "include.query";
    private static final String TRUE = "true";

    private final Severity severity;

    public ConnectDebeziumMysqlIncludeQueryTrueRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_MYSQL_INCLUDE_QUERY_TRUE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MYSQL_CONNECTOR.equals(connectorClass)) continue;

            String raw = trimOrNull(p.getProperty(INCLUDE_QUERY));
            if (raw == null) continue;
            if (!TRUE.equals(raw.toLowerCase(Locale.ROOT))) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_MYSQL_INCLUDE_QUERY_TRUE, severity,
                    ctx.relativize(e.getKey()), "key:" + INCLUDE_QUERY, 0,
                    "Debezium MySQL connector has include.query=true — the connector "
                            + "EMBEDS the RAW SQL STATEMENT that produced each change "
                            + "into the emitted Debezium envelope's source.query field. "
                            + "The SQL is taken VERBATIM from the binlog: full "
                            + "statement, full parameter values INLINED into the SQL "
                            + "string (NOT bound separately as placeholders), comments "
                            + "included, NO redaction, NO PII-masking. Every "
                            + "downstream consumer of the CDC topic then has READ "
                            + "access to potentially-sensitive SQL text — PII in "
                            + "WHERE/SET clauses (email addresses, social-security "
                            + "numbers, passwords-being-set, payment-card numbers, "
                            + "customer names), proprietary application internals (the "
                            + "application's exact SQL queries, schema-level details), "
                            + "audit-noise (debug-level admin queries with internal "
                            + "context), and compliance-violating content. Typical CDC "
                            + "governance controls (row-level masking, column-level "
                            + "encryption) are applied to the after.* fields, NOT to "
                            + "source.query — so masked sensitive columns LEAK in the "
                            + "unmasked SQL text. The CDC topic typically replicates "
                            + "via sink connectors to long-term retention stores (S3, "
                            + "BigQuery, Snowflake) where the SQL persists for years — "
                            + "a permanent data leak with regulatory consequences "
                            + "(GDPR Article 32, PCI-DSS data-minimization, HIPAA log "
                            + "handling). Fix: set include.query=false (the default), "
                            + "or remove the key. The legitimate use of "
                            + "include.query=true is narrow: transient "
                            + "development-time debugging where the operator wants to "
                            + "inspect the exact SQL in kafka-console-consumer output. "
                            + "In production this setting is almost always a forgotten "
                            + "debug-time flag that becomes a permanent privacy/"
                            + "security leak. Common origins: (a) 'I'll just turn it "
                            + "on for debugging' — the debugging session ends, the "
                            + "setting is forgotten; (b) a Helm chart with "
                            + "'debug-friendly' default values inherited org-wide; "
                            + "(c) dev-config copy-pasted to production. Note: "
                            + "include.query is a no-op unless the MySQL server is "
                            + "running with binlog_rows_query_log_events=ON — but the "
                            + "INTENT to enable query inclusion is the security "
                            + "concern, and the leak begins the moment the server's "
                            + "setting is flipped (or a different replica with the "
                            + "setting ON is added to the source pool)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
