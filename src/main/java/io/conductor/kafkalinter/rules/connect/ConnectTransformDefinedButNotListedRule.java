package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * declares {@code transforms.<alias>.<key>=<value>} entries for an alias that
 * is NOT listed in the {@code transforms=} chain. Connect's SMT machinery
 * silently ignores orphan {@code transforms.<X>.<Y>} entries — no error,
 * no warning — so the operator's transform is not applied at runtime.
 *
 * <p>This is the inverse of {@link ConnectTransformAliasUndefinedRule}:
 * that rule catches aliases listed but unbound (runtime ConfigException);
 * THIS rule catches bindings without a listed alias (silent no-op).
 *
 * <p>One violation per orphan alias per file (subkeys for the same alias
 * are aggregated into a single violation message).
 */
public final class ConnectTransformDefinedButNotListedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TRANSFORMS_LIST = "transforms";
    private static final String TRANSFORMS_PREFIX = "transforms.";

    private final Severity severity;

    public ConnectTransformDefinedButNotListedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_TRANSFORM_DEFINED_BUT_NOT_LISTED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            Set<String> declaredAliases = parseChain(p.getProperty(TRANSFORMS_LIST));

            LinkedHashMap<String, Set<String>> orphanSubkeysByAlias = new LinkedHashMap<>();
            for (String key : p.stringPropertyNames()) {
                if (!key.startsWith(TRANSFORMS_PREFIX)) continue;
                String tail = key.substring(TRANSFORMS_PREFIX.length());
                int dotIdx = tail.indexOf('.');
                if (dotIdx <= 0) continue;
                String alias = tail.substring(0, dotIdx);
                String subkey = tail.substring(dotIdx + 1);
                if (subkey.isEmpty()) continue;
                if (declaredAliases.contains(alias)) continue;

                orphanSubkeysByAlias.computeIfAbsent(alias, a -> new TreeSet<>()).add(subkey);
            }

            for (Map.Entry<String, Set<String>> orphan : orphanSubkeysByAlias.entrySet()) {
                String alias = orphan.getKey();
                String subkeys = String.join(", ", orphan.getValue());
                String chainDisplay = p.getProperty(TRANSFORMS_LIST) == null
                        ? "<unset>" : p.getProperty(TRANSFORMS_LIST).trim();
                out.add(new Violation(
                        RuleId.CONNECT_TRANSFORM_DEFINED_BUT_NOT_LISTED, severity,
                        ctx.relativize(e.getKey()), "key:transforms." + alias + ".type", 0,
                        "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                                + " defines transforms." + alias + ".* entries (subkeys: "
                                + subkeys + ") but '" + alias + "' is NOT listed in transforms="
                                + chainDisplay + ". Connect's SMT framework iterates ONLY the "
                                + "aliases in the transforms= chain — every transforms." + alias
                                + ".<key> entry is SILENTLY IGNORED at runtime (no error log, "
                                + "no warning, no ConfigException). The operator believes the "
                                + "transform is applied (the config is present) but the runtime "
                                + "is not applying it. This is the #1 cause of 'why is my "
                                + "MaskField transform not masking PII?' / 'why is my "
                                + "RegexRouter not renaming topics?' Connect operator "
                                + "confusions. Either add '" + alias + "' to the transforms= "
                                + "chain (so the SMT actually runs), OR remove the orphan "
                                + "transforms." + alias + ".* entries (so the config reflects "
                                + "the actual runtime behavior — no transform applied)."));
            }
        }
        return out;
    }

    private static Set<String> parseChain(String raw) {
        if (!notBlank(raw)) return Set.of();
        Set<String> out = new HashSet<>();
        for (String token : raw.split(",")) {
            String t = token.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
