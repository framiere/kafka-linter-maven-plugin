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
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * declares {@code connector.class=} but does NOT set {@code name=}.
 *
 * <p>Connect requires {@code name} as the connector's identity for the REST
 * API resource path, consumer group ID derivation (sinks), internal storage
 * namespacing, status/log correlation, and DLQ provenance headers. A config
 * without {@code name} fails registration with
 * {@code ConfigException: Missing required configuration "name"}.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectNameMissingRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String NAME = "name";

    private final Severity severity;

    public ConnectNameMissingRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_NAME_MISSING;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;
            if (notBlank(p.getProperty(NAME))) continue;

            String state = (p.getProperty(NAME) == null) ? "not set" : "blank";

            out.add(new Violation(
                    RuleId.CONNECT_NAME_MISSING, severity,
                    ctx.relativize(e.getKey()), "key:" + CONNECTOR_CLASS, 0,
                    "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                            + " is declared but the required 'name' property is " + state
                            + ". Connect requires 'name' as the connector's identity for the "
                            + "REST API resource path (PUT /connectors/<name>/config), the "
                            + "consumer group ID derivation (sinks default to "
                            + "'connect-<name>'), internal storage namespacing, status/log "
                            + "correlation, and DLQ provenance headers. Connect rejects the "
                            + "config at parse time with 'ConfigException: Missing required "
                            + "configuration \"name\" which has no default value'. Add "
                            + "name=<unique-identifier> (kebab-case is conventional, e.g., "
                            + "name=jdbc-sink-orders-prod)."));
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
