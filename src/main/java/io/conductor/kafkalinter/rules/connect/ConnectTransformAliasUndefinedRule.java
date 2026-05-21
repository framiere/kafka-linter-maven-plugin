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
 * has BOTH:
 * <ul>
 *   <li>a {@code connector.class} key (the canonical Connect-config fingerprint), AND</li>
 *   <li>a non-empty {@code transforms=A,B,C} chain whose at least one alias has
 *       no matching {@code transforms.<alias>.type=<SMT-class>} entry (or has it
 *       set to blank).</li>
 * </ul>
 *
 * <p>Every alias in the {@code transforms=} list must have a corresponding
 * {@code transforms.<alias>.type} key that names the fully-qualified SMT class.
 * A missing alias-to-class binding fails the task at startup with a precise
 * but easy-to-misread ConfigException — this rule catches the typo at
 * static-analysis time.
 *
 * <p>Emits one violation per offending alias (so a file with three undefined
 * aliases emits three violations).
 */
public final class ConnectTransformAliasUndefinedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TRANSFORMS_LIST = "transforms";

    private final Severity severity;

    public ConnectTransformAliasUndefinedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_TRANSFORM_ALIAS_UNDEFINED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;
            String transformsRaw = p.getProperty(TRANSFORMS_LIST);
            if (!notBlank(transformsRaw)) continue;

            for (String rawAlias : transformsRaw.split(",")) {
                String alias = rawAlias.trim();
                if (alias.isEmpty()) continue;
                String typeKey = "transforms." + alias + ".type";
                if (notBlank(p.getProperty(typeKey))) continue;

                out.add(new Violation(
                        RuleId.CONNECT_TRANSFORM_ALIAS_UNDEFINED, severity,
                        ctx.relativize(e.getKey()), "key:" + typeKey, 0,
                        "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                                + " lists transform alias '" + alias + "' in transforms="
                                + transformsRaw.trim() + " but does NOT define "
                                + typeKey + "=<SMT-class> — the task will fail at startup with "
                                + "ConfigException 'Missing required configuration \"" + typeKey
                                + "\"'. Either add " + typeKey + "=<fully-qualified-SMT-class> "
                                + "with the SMT's config keys at transforms." + alias + ".<key>, "
                                + "or remove '" + alias + "' from the transforms= list."));
            }
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
