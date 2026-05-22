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
 * Project-scoped rule. Fires when a Confluent JDBC source connector
 * ({@code io.confluent.connect.jdbc.JdbcSourceConnector}) is configured
 * with {@code validate.non.null=false} AND a non-bulk {@code mode}
 * ({@code incrementing}, {@code timestamp}, or
 * {@code timestamp+incrementing}).
 *
 * <p>The default {@code validate.non.null=true} forces the connector to
 * fail at startup if the configured tracking column is nullable —
 * preventing silent data loss. Setting it to {@code false} disables the
 * check; the connector starts, but rows with NULL values in the tracking
 * column are silently filtered out by SQL three-valued logic in the
 * polling WHERE clause and NEVER emitted to Kafka.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectJdbcSourceValidateNonNullFalseRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String JDBC_SOURCE_CLASS = "io.confluent.connect.jdbc.JdbcSourceConnector";
    private static final String VALIDATE_NON_NULL = "validate.non.null";
    private static final String MODE = "mode";

    private final Severity severity;

    public ConnectJdbcSourceValidateNonNullFalseRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_JDBC_SOURCE_VALIDATE_NON_NULL_FALSE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (!JDBC_SOURCE_CLASS.equals(connectorClass)) continue;

            String validate = trimOrNull(p.getProperty(VALIDATE_NON_NULL));
            if (validate == null) continue;
            if (!"false".equals(validate.toLowerCase(Locale.ROOT))) continue;

            String mode = trimOrNull(p.getProperty(MODE));
            if (mode == null) continue;
            String modeLc = mode.toLowerCase(Locale.ROOT);
            if (!(modeLc.equals("incrementing")
                    || modeLc.equals("timestamp")
                    || modeLc.equals("timestamp+incrementing"))) continue;

            out.add(new Violation(
                    RuleId.CONNECT_JDBC_SOURCE_VALIDATE_NON_NULL_FALSE, severity,
                    ctx.relativize(e.getKey()), "key:" + VALIDATE_NON_NULL, 0,
                    "Confluent JDBC source connector has validate.non.null=" + validate
                            + " with mode=" + mode + ". The default validate.non.null=true "
                            + "exists to FAIL the connector at startup if the configured "
                            + "tracking column (incrementing.column.name or "
                            + "timestamp.column.name) is nullable on the source table — "
                            + "preventing silent data loss. Setting it to false disables "
                            + "that check; the connector starts, but the polling SQL "
                            + "WHERE clause `WHERE <tracking-column> > ? ORDER BY "
                            + "<tracking-column> ASC` SILENTLY EXCLUDES every row where "
                            + "the tracking column is NULL (SQL three-valued logic: NULL "
                            + "> X is NULL, not TRUE — so the WHERE clause filters those "
                            + "rows out). The skipped rows are NEVER emitted to Kafka; "
                            + "there is no error, no warning, no DLQ entry, no metric: "
                            + "the rows simply do not appear in the topic. Downstream "
                            + "pipelines under-count rows, analytics dashboards diverge "
                            + "from source-DB COUNT(*), regulatory reporting falls short, "
                            + "and the discrepancy can persist for years before "
                            + "reconciliation surfaces it. The connector's OWN error "
                            + "message at startup ('set validate.non.null=false to ignore "
                            + "this requirement') is what causes most operators to set "
                            + "this flag — the message reads as a normal config-tuning "
                            + "suggestion, not as a silent-data-loss switch. Fix: (a) "
                            + "backfill the NULL values in the source table and add a "
                            + "NOT NULL constraint (the operationally-correct fix); (b) "
                            + "choose a different tracking column that is NOT NULL (e.g., "
                            + "switch from a nullable updated_at to an auto-increment "
                            + "surrogate-key id); (c) keep validate.non.null=true and "
                            + "accept the loud-failure mode at startup until the source "
                            + "table is fixed; (d) explicitly document the accepted "
                            + "data-loss surface (rarely appropriate in production CDC)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
