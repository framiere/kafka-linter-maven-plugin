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
 * Project-scoped rule. Fires when a Kafka Connect connector's SMT chain
 * declares {@code transforms.<alias>.type=org.apache.kafka.connect.transforms.RegexRouter}
 * but is missing {@code transforms.<alias>.regex} or
 * {@code transforms.<alias>.replacement} (or both).
 *
 * <p>RegexRouter requires BOTH keys; neither has a default. When either is
 * missing, the SMT fails to configure at task startup with
 * {@code ConfigException: Missing required configuration "<key>"}.
 *
 * <p>Emits one violation per (file, alias, missing-key) pair — a chain with
 * BOTH keys missing produces TWO violations for the same alias for maximum
 * diagnostic clarity.
 */
public final class ConnectTransformRegexRouterMissingRegexOrReplacementRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TRANSFORMS = "transforms";
    private static final String REGEX_ROUTER = "org.apache.kafka.connect.transforms.RegexRouter";

    private final Severity severity;

    public ConnectTransformRegexRouterMissingRegexOrReplacementRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_TRANSFORM_REGEXROUTER_MISSING_REGEX_OR_REPLACEMENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            String chain = p.getProperty(TRANSFORMS);
            if (!notBlank(chain)) continue;

            for (String rawAlias : chain.split(",")) {
                String alias = rawAlias.trim();
                if (alias.isEmpty()) continue;

                String typeKey = TRANSFORMS + "." + alias + ".type";
                String type = trimOrNull(p.getProperty(typeKey));
                if (!REGEX_ROUTER.equals(type)) continue;

                String regexKey = TRANSFORMS + "." + alias + ".regex";
                String replacementKey = TRANSFORMS + "." + alias + ".replacement";

                if (!notBlank(p.getProperty(regexKey))) {
                    out.add(buildViolation(ctx, e.getKey(), p, alias, regexKey, "regex"));
                }
                if (!notBlank(p.getProperty(replacementKey))) {
                    out.add(buildViolation(ctx, e.getKey(), p, alias, replacementKey, "replacement"));
                }
            }
        }
        return out;
    }

    private Violation buildViolation(ProjectContext ctx, Path file, Properties p,
                                     String alias, String missingKey, String missingName) {
        return new Violation(
                RuleId.CONNECT_TRANSFORM_REGEXROUTER_MISSING_REGEX_OR_REPLACEMENT, severity,
                ctx.relativize(file), "key:" + missingKey, 0,
                "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                        + " declares transforms." + alias + ".type=" + REGEX_ROUTER
                        + " but is missing the required transforms." + alias + "."
                        + missingName + " key. RegexRouter REQUIRES BOTH regex= (the Java "
                        + "regex pattern matched against the record's destination topic name) "
                        + "AND replacement= (the substitution string, with $1, $2, etc. "
                        + "back-references); neither has a default. The SMT fails to configure "
                        + "at task startup with 'ConfigException: Missing required configuration "
                        + "\"" + missingName + "\" which has no default value.' The connector "
                        + "task never starts; the worker reports a FAILED status. Add "
                        + missingKey + "=<value>.");
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
