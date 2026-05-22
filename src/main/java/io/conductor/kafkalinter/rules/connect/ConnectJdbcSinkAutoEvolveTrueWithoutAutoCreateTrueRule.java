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
 * ({@code io.confluent.connect.jdbc.JdbcSinkConnector}) sets
 * {@code auto.evolve=true} but does NOT set {@code auto.create=true}
 * (either absent or any value other than {@code true}).
 *
 * <p>The two configs are jointly constrained by intent: either the
 * connector owns the downstream schema lifecycle (both {@code true})
 * or the DBA owns it (both absent / {@code false}). The mixed
 * configuration {@code auto.evolve=true, auto.create=false} is
 * structurally inconsistent — the connector evolves existing tables
 * but cannot create new ones; new topics fail to deliver records.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectJdbcSinkAutoEvolveTrueWithoutAutoCreateTrueRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String JDBC_SINK_CONNECTOR_CLASS = "io.confluent.connect.jdbc.JdbcSinkConnector";
    private static final String AUTO_EVOLVE = "auto.evolve";
    private static final String AUTO_CREATE = "auto.create";

    private final Severity severity;

    public ConnectJdbcSinkAutoEvolveTrueWithoutAutoCreateTrueRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_JDBC_SINK_AUTO_EVOLVE_TRUE_WITHOUT_AUTO_CREATE_TRUE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (!JDBC_SINK_CONNECTOR_CLASS.equals(connectorClass)) continue;

            String autoEvolve = trimOrNull(p.getProperty(AUTO_EVOLVE));
            if (autoEvolve == null) continue;
            if (!"true".equals(autoEvolve.toLowerCase(Locale.ROOT))) continue;

            String autoCreate = trimOrNull(p.getProperty(AUTO_CREATE));
            String autoCreateLower = autoCreate == null ? null : autoCreate.toLowerCase(Locale.ROOT);
            if ("true".equals(autoCreateLower)) continue;

            String autoCreateDesc = autoCreate == null ? "absent (defaults to false)" : "set to `" + autoCreate + "`";
            out.add(new Violation(
                    RuleId.CONNECT_JDBC_SINK_AUTO_EVOLVE_TRUE_WITHOUT_AUTO_CREATE_TRUE, severity,
                    ctx.relativize(e.getKey()), "key:" + AUTO_EVOLVE, 0,
                    "Confluent JDBC sink connector has auto.evolve=true but auto.create is " + autoCreateDesc
                            + ". The two configs are JOINTLY constrained by intent: with auto.evolve=true "
                            + "the connector takes ownership of evolving existing tables (issues ALTER "
                            + "TABLE ADD COLUMN when an upstream Avro/JSON/Protobuf schema adds a new "
                            + "field), but with auto.create=false (or absent — default `false`) the "
                            + "connector does NOT take ownership of creating new tables. The downstream "
                            + "pipeline is brittle: a NEW topic introduced upstream (e.g. a new entity "
                            + "type emitted by an event-sourcing service, a new table added by another "
                            + "team) will FAIL the connector with `Table <name> is missing` until a "
                            + "DBA manually creates the table; meanwhile, EXISTING tables silently "
                            + "drift away from DBA control via the connector's ALTER TABLE statements. "
                            + "The mixed configuration has no legitimate operational context. Fix: "
                            + "either set auto.create=true (development / sandbox — the connector owns "
                            + "the full table lifecycle), or set auto.evolve=false / remove it "
                            + "(production — the DBA owns the schema; missing tables and columns are "
                            + "visible errors that prompt a coordinated DDL migration). Sibling rules: "
                            + "CONNECT_JDBC_SINK_UPSERT_OR_UPDATE_WITHOUT_PK, "
                            + "CONNECT_JDBC_SINK_DELETE_ENABLED_TRUE_WITHOUT_PK_MODE_RECORD_KEY — same "
                            + "connector, same config-consistency pattern."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
