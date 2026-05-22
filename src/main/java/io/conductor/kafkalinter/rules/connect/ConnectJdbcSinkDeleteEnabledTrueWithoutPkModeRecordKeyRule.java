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
 * with {@code delete.enabled=true} but {@code pk.mode} is NOT
 * {@code record_key}.
 *
 * <p>The {@code delete.enabled=true} feature translates Kafka tombstone
 * records (non-null key, null value) into SQL DELETE statements. Since
 * the value is null by definition for a tombstone, the PK must come from
 * the record KEY — hence {@code pk.mode=record_key} is the only valid
 * pairing. The connector enforces this with a hard cross-key constraint
 * (the task throws {@code ConfigException} at start when violated).
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectJdbcSinkDeleteEnabledTrueWithoutPkModeRecordKeyRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String JDBC_SINK_CLASS = "io.confluent.connect.jdbc.JdbcSinkConnector";
    private static final String DELETE_ENABLED = "delete.enabled";
    private static final String PK_MODE = "pk.mode";

    private final Severity severity;

    public ConnectJdbcSinkDeleteEnabledTrueWithoutPkModeRecordKeyRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_JDBC_SINK_DELETE_ENABLED_TRUE_WITHOUT_PK_MODE_RECORD_KEY;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (!JDBC_SINK_CLASS.equals(connectorClass)) continue;

            String deleteEnabled = trimOrNull(p.getProperty(DELETE_ENABLED));
            if (deleteEnabled == null) continue;
            if (!"true".equals(deleteEnabled.toLowerCase(Locale.ROOT))) continue;

            String pkMode = trimOrNull(p.getProperty(PK_MODE));
            String pkModeLc = pkMode == null ? null : pkMode.toLowerCase(Locale.ROOT);
            if ("record_key".equals(pkModeLc)) continue;

            String observedPkMode = pkMode == null ? "(absent — defaults to 'none')" : pkMode;
            out.add(new Violation(
                    RuleId.CONNECT_JDBC_SINK_DELETE_ENABLED_TRUE_WITHOUT_PK_MODE_RECORD_KEY, severity,
                    ctx.relativize(e.getKey()), "key:" + PK_MODE, 0,
                    "Confluent JDBC sink connector has delete.enabled=true but pk.mode="
                            + observedPkMode
                            + ". The delete.enabled=true feature translates Kafka TOMBSTONE records "
                            + "(non-null key, NULL value) into SQL DELETE statements against the "
                            + "target table. Because the value is NULL by definition for a tombstone, "
                            + "the PK MUST be extracted from the record KEY — so pk.mode=record_key is "
                            + "the ONLY valid pairing. The other pk.mode values are incompatible: "
                            + "pk.mode=none (no PK at all — DELETE WHERE clause impossible); "
                            + "pk.mode=kafka (synthetic PK from topic+partition+offset — doesn't "
                            + "match source-DB row identity, DELETE would target nothing or worse, a "
                            + "wrong row); pk.mode=record_value (PK in the value envelope, but the "
                            + "value is NULL for a tombstone — nothing to extract). The connector's "
                            + "start() throws `ConfigException: pk.mode must be record_key when "
                            + "delete.enabled is true`; the task transitions to FAILED while the "
                            + "connector itself reports RUNNING. Fix: set pk.mode=record_key + "
                            + "pk.fields=<the-key-field-name(s)> — the canonical CDC-sink "
                            + "configuration that pairs with an upstream Debezium connector emitting "
                            + "source-row PKs in the Kafka record key, with tombstones for source "
                            + "DELETE events."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
