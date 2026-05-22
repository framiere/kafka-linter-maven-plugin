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
 * Project-scoped rule. Fires when a Confluent JDBC sink connector
 * ({@code io.confluent.connect.jdbc.JdbcSinkConnector}) is configured
 * with {@code insert.mode=upsert} or {@code insert.mode=update} but the
 * primary-key configuration is missing or incomplete.
 *
 * <p>Detection matrix (only the {@code upsert} and {@code update} insert
 * modes are checked; {@code insert} mode has no PK requirement):
 * <ul>
 *   <li>{@code pk.mode} absent or {@code none} → fire (missing pk.mode)</li>
 *   <li>{@code pk.mode} is {@code record_key} or {@code record_value} AND
 *       {@code pk.fields} is absent or empty → fire (missing pk.fields)</li>
 *   <li>{@code pk.mode=kafka} → silent (technically valid, but rarely the
 *       operator's intent — covered by a future advisory rule)</li>
 * </ul>
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectJdbcSinkUpsertOrUpdateWithoutPkRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String JDBC_SINK_CLASS = "io.confluent.connect.jdbc.JdbcSinkConnector";
    private static final String INSERT_MODE = "insert.mode";
    private static final String PK_MODE = "pk.mode";
    private static final String PK_FIELDS = "pk.fields";

    private final Severity severity;

    public ConnectJdbcSinkUpsertOrUpdateWithoutPkRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_JDBC_SINK_UPSERT_OR_UPDATE_WITHOUT_PK;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (!JDBC_SINK_CLASS.equals(connectorClass)) continue;

            String insertMode = trimOrNull(p.getProperty(INSERT_MODE));
            if (insertMode == null) continue;
            String insertModeLc = insertMode.toLowerCase(Locale.ROOT);
            if (!(insertModeLc.equals("upsert") || insertModeLc.equals("update"))) continue;

            String pkMode = trimOrNull(p.getProperty(PK_MODE));
            String pkModeLc = pkMode == null ? null : pkMode.toLowerCase(Locale.ROOT);

            String violationDetail = null;
            String anchorKey = null;
            if (pkModeLc == null || pkModeLc.equals("none")) {
                anchorKey = "key:" + PK_MODE;
                violationDetail = "pk.mode=" + (pkMode == null ? "(absent — defaults to 'none')" : pkMode)
                        + " is incompatible with insert.mode=" + insertMode
                        + ". The connector cannot construct the WHERE clause for upsert/update SQL "
                        + "without a primary key. Set pk.mode=record_key (the standard for CDC sinks "
                        + "consuming from a Debezium source — extracts the PK from the Kafka record "
                        + "key) and pk.fields=<the-key-field-name(s)>; OR pk.mode=record_value + "
                        + "pk.fields=<the-PK-field-in-the-value-envelope> when the value carries the "
                        + "PK; OR switch insert.mode=insert if the sink is supposed to be append-only.";
            } else if (pkModeLc.equals("record_key") || pkModeLc.equals("record_value")) {
                String pkFields = trimOrNull(p.getProperty(PK_FIELDS));
                if (pkFields == null || pkFields.isEmpty()) {
                    anchorKey = "key:" + PK_FIELDS;
                    violationDetail = "pk.fields is " + (pkFields == null ? "absent" : "empty")
                            + " but pk.mode=" + pkMode + " requires a non-empty CSV of column "
                            + "name(s). The connector knows WHERE to look for the PK (the record "
                            + (pkModeLc.equals("record_key") ? "key" : "value")
                            + ") but does NOT know WHICH field(s) form the PK. Set pk.fields=<col1>"
                            + "[,<col2>,...] naming the column(s) in the record "
                            + (pkModeLc.equals("record_key") ? "key" : "value")
                            + " envelope that uniquely identify the target row.";
                }
            }

            if (violationDetail == null) continue;

            out.add(new Violation(
                    RuleId.CONNECT_JDBC_SINK_UPSERT_OR_UPDATE_WITHOUT_PK, severity,
                    ctx.relativize(e.getKey()), anchorKey, 0,
                    "Confluent JDBC sink connector has insert.mode=" + insertMode
                            + " but the primary-key configuration is missing or incomplete: "
                            + violationDetail
                            + " The connector's start() throws ConfigException; the task transitions "
                            + "to FAILED while the CONNECTOR state remains RUNNING (Connect's state "
                            + "model lets a connector be RUNNING while all tasks are FAILED) — "
                            + "visible only in the per-task status REST endpoint, not in the "
                            + "connector-status response that operators typically monitor. The result "
                            + "is a sink that LOOKS healthy in dashboards while NO rows are written "
                            + "to the target table; downstream analytics, regulatory reporting, and "
                            + "operational systems silently fall out of sync with the source. The "
                            + "canonical CDC-sink configuration is `insert.mode=upsert` + "
                            + "`pk.mode=record_key` + `pk.fields=<the-Debezium-key-field-name(s)>` "
                            + "(extract the PK from the Kafka record key, which the upstream Debezium "
                            + "source populates with the source-table PK)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
