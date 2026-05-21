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
 * declares {@code tasks.max=} with a value that is zero, negative, or
 * non-numeric.
 *
 * <p>Connect requires {@code tasks.max} to be a positive integer
 * ({@code ConfigDef.Range.atLeast(1)}). Invalid values fail at config-parse
 * time with {@code ConfigException}; the connector never registers.
 *
 * <p>This rule does NOT fire when {@code tasks.max} is absent or blank —
 * Connect defaults to 1, which is valid (though a future rule may flag it
 * for high-throughput connectors).
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectTasksMaxLessThanOneRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TASKS_MAX = "tasks.max";

    private final Severity severity;

    public ConnectTasksMaxLessThanOneRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_TASKS_MAX_LESS_THAN_ONE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            String raw = p.getProperty(TASKS_MAX);
            if (raw == null) continue;
            String trimmed = raw.trim();
            if (trimmed.isEmpty()) continue;

            Integer parsed = tryParseInt(trimmed);
            String issue;
            if (parsed == null) {
                issue = "is not a valid integer ('" + trimmed + "')";
            } else if (parsed < 1) {
                issue = "= " + parsed + " is less than 1";
            } else {
                continue;
            }

            out.add(new Violation(
                    RuleId.CONNECT_TASKS_MAX_LESS_THAN_ONE, severity,
                    ctx.relativize(e.getKey()), "key:" + TASKS_MAX, 0,
                    "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                            + " has tasks.max " + issue
                            + " — Connect requires tasks.max to be a positive integer "
                            + "(ConfigDef.Range.atLeast(1)). Connect rejects the config at "
                            + "parse time with 'ConfigException: " + (parsed == null
                                    ? "Not a number of type INT"
                                    : "Value must be at least 1, was " + parsed)
                            + "'. The connector cannot register; the worker logs the failure; "
                            + "no records are processed. Common trigger: a Helm-template "
                            + "variable defaults to zero, an env-var substitution produces an "
                            + "empty string, or a typo (letter O instead of digit zero). Set "
                            + "tasks.max=<positive-integer>."));
        }
        return out;
    }

    private static Integer tryParseInt(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
