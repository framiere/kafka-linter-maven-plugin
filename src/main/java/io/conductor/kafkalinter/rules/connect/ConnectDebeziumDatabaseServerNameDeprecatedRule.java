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
 * Project-scoped rule. Fires when a Debezium connector
 * ({@code connector.class} starts with {@code io.debezium.connector.})
 * has the legacy Debezium 1.x key {@code database.server.name} set.
 *
 * <p>In Debezium 2.0 this key was renamed to {@code topic.prefix}
 * (identical semantics). In Debezium 2.0-2.4 it was a deprecated alias
 * with a WARN log; in 2.5+ it was removed entirely.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumDatabaseServerNameDeprecatedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";
    private static final String DATABASE_SERVER_NAME = "database.server.name";

    private final Severity severity;

    public ConnectDebeziumDatabaseServerNameDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_DATABASE_SERVER_NAME_DEPRECATED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;

            String legacy = trimOrNull(p.getProperty(DATABASE_SERVER_NAME));
            if (legacy == null) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_DATABASE_SERVER_NAME_DEPRECATED, severity,
                    ctx.relativize(e.getKey()), "key:" + DATABASE_SERVER_NAME, 0,
                    "Debezium connector has legacy Debezium 1.x key "
                            + "database.server.name=" + legacy + " — this key was "
                            + "RENAMED to topic.prefix in Debezium 2.0 (semantics "
                            + "IDENTICAL: same value, same role as topic-name prefix "
                            + "and as logical-source identifier in the Debezium "
                            + "envelope's source.name field — but the name was "
                            + "clarified to reflect that this is fundamentally a "
                            + "topic-naming concern, not a database-server-naming "
                            + "concern). The legacy key was kept as a deprecated "
                            + "alias in Debezium 2.0-2.4 (the connector accepts it, "
                            + "logs a WARN, uses its value as a fallback when "
                            + "topic.prefix is absent) and REMOVED entirely from the "
                            + "connector's ConfigDef in Debezium 2.5. In Debezium "
                            + "2.5+ Connect silently accepts the unknown key (the "
                            + "REST-API PUT-validate endpoint does NOT flag it); the "
                            + "connector's REQUIRED topic.prefix config is absent; "
                            + "the connector fails at start with "
                            + "ConfigException: topic.prefix is required. Fix: "
                            + "replace database.server.name=" + legacy + " with "
                            + "topic.prefix=" + legacy + " (same value, just rename "
                            + "the key). If both keys are currently set (a safe "
                            + "migration intermediate), remove the legacy key — "
                            + "carrying both forward is cruft that future "
                            + "maintainers may misinterpret, and in 2.5+ the legacy "
                            + "key has no effect. The migration has NO downstream "
                            + "impact (the emitted topic names and the source.name "
                            + "field values are identical) and no Kafka-side "
                            + "changes. Common origins: (a) Debezium 1.x config "
                            + "carried forward to a 2.x deployment without rename; "
                            + "(b) copy-paste from a 2019-era tutorial; (c) a Helm "
                            + "chart from the 1.x era 'upgraded' by bumping only "
                            + "the image version."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
