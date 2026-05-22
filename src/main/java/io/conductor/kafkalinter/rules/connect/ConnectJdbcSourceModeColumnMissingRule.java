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
 * Project-scoped rule. Fires when a Confluent JDBC source connector
 * (`io.confluent.connect.jdbc.JdbcSourceConnector`) is configured with
 * `mode=incrementing`, `mode=timestamp`, or `mode=timestamp+incrementing` but
 * the required column-name key(s) are missing or blank.
 *
 * <p>The mode/column-name pairing is:
 * <ul>
 *   <li>{@code bulk} → no column required</li>
 *   <li>{@code incrementing} → {@code incrementing.column.name} required</li>
 *   <li>{@code timestamp} → {@code timestamp.column.name} required</li>
 *   <li>{@code timestamp+incrementing} → BOTH required</li>
 * </ul>
 *
 * <p>The Confluent connector's ConfigDef does not validate this pairing — the
 * cross-key check is implemented in {@code JdbcSourceConnector#start()} which
 * runs AFTER REST submission, so a misconfigured connector submits cleanly,
 * goes to RUNNING state, then has its first task transition to FAILED with a
 * non-obvious error. Catching the mismatch statically prevents that.
 */
public final class ConnectJdbcSourceModeColumnMissingRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String JDBC_SOURCE_CLASS = "io.confluent.connect.jdbc.JdbcSourceConnector";
    private static final String MODE = "mode";
    private static final String INCREMENTING_COL = "incrementing.column.name";
    private static final String TIMESTAMP_COL = "timestamp.column.name";

    private final Severity severity;

    public ConnectJdbcSourceModeColumnMissingRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_JDBC_SOURCE_MODE_COLUMN_MISSING;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (!JDBC_SOURCE_CLASS.equals(connectorClass)) continue;

            String mode = trimOrNull(p.getProperty(MODE));
            if (mode == null) continue;
            String modeLc = mode.toLowerCase();

            boolean needsIncrementing = modeLc.equals("incrementing") || modeLc.equals("timestamp+incrementing");
            boolean needsTimestamp = modeLc.equals("timestamp") || modeLc.equals("timestamp+incrementing");
            if (!needsIncrementing && !needsTimestamp) continue;

            if (needsIncrementing && trimOrNull(p.getProperty(INCREMENTING_COL)) == null) {
                out.add(new Violation(
                        RuleId.CONNECT_JDBC_SOURCE_MODE_COLUMN_MISSING, severity,
                        ctx.relativize(e.getKey()), "key:" + INCREMENTING_COL, 0,
                        "Confluent JDBC source connector configured with mode=" + mode
                                + " but " + INCREMENTING_COL + " is missing or blank. The "
                                + "connector task will fail at startup with ConfigException: "
                                + "'Must specify a column name for the " + modeLc + " mode.' "
                                + "— but the failure hides behind the task-status endpoint "
                                + "while the connector-status remains RUNNING, so the operator "
                                + "sees 'connector running' and may debug downstream before "
                                + "noticing the config error. Set " + INCREMENTING_COL
                                + "=<your-monotonic-integer-column> (typically an auto-increment "
                                + "PK), OR change mode=bulk if you actually want a full table "
                                + "snapshot on every poll."));
            }
            if (needsTimestamp && trimOrNull(p.getProperty(TIMESTAMP_COL)) == null) {
                out.add(new Violation(
                        RuleId.CONNECT_JDBC_SOURCE_MODE_COLUMN_MISSING, severity,
                        ctx.relativize(e.getKey()), "key:" + TIMESTAMP_COL, 0,
                        "Confluent JDBC source connector configured with mode=" + mode
                                + " but " + TIMESTAMP_COL + " is missing or blank. The "
                                + "connector task will fail at startup with ConfigException: "
                                + "'Must specify a column name for the " + modeLc + " mode.' "
                                + "— but the failure hides behind the task-status endpoint "
                                + "while the connector-status remains RUNNING, so the operator "
                                + "sees 'connector running' and may debug downstream before "
                                + "noticing the config error. Set " + TIMESTAMP_COL
                                + "=<your-timestamp-column>[,<col2>,...] (a column whose value "
                                + "monotonically increases as rows are updated, typically "
                                + "updated_at), OR change mode=bulk if you actually want a "
                                + "full table snapshot on every poll."));
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
