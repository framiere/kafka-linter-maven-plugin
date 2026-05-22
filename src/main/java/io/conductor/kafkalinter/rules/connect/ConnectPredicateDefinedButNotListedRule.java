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
 * declares {@code predicates.<alias>.<key>=<value>} entries for an alias that
 * is NOT listed in the {@code predicates=} chain. Connect's KIP-585 predicate
 * machinery silently ignores orphan {@code predicates.<X>.<Y>} entries — no
 * error, no warning — so the predicate is never registered at runtime.
 *
 * <p>This is the symmetric pair of {@link ConnectTransformDefinedButNotListedRule}
 * (which catches dangling {@code transforms.<X>.<Y>} entries) and the inverse
 * of {@link ConnectPredicateReferenceUndefinedRule} (which catches SMT-side
 * references to undefined predicates — those fail at startup with
 * ConfigException; the dangling-definition case caught here is silent).
 *
 * <p>One violation per orphan alias per file (subkeys for the same alias are
 * aggregated into a single violation message).
 */
public final class ConnectPredicateDefinedButNotListedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PREDICATES_LIST = "predicates";
    private static final String PREDICATES_PREFIX = "predicates.";

    private final Severity severity;

    public ConnectPredicateDefinedButNotListedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_PREDICATE_DEFINED_BUT_NOT_LISTED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            Set<String> declaredAliases = parseChain(p.getProperty(PREDICATES_LIST));

            LinkedHashMap<String, Set<String>> orphanSubkeysByAlias = new LinkedHashMap<>();
            for (String key : p.stringPropertyNames()) {
                if (!key.startsWith(PREDICATES_PREFIX)) continue;
                String tail = key.substring(PREDICATES_PREFIX.length());
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
                String chainDisplay = p.getProperty(PREDICATES_LIST) == null
                        ? "<unset>" : p.getProperty(PREDICATES_LIST).trim();
                out.add(new Violation(
                        RuleId.CONNECT_PREDICATE_DEFINED_BUT_NOT_LISTED, severity,
                        ctx.relativize(e.getKey()), "key:predicates." + alias + ".type", 0,
                        "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                                + " defines predicates." + alias + ".* entries (subkeys: "
                                + subkeys + ") but '" + alias + "' is NOT listed in predicates="
                                + chainDisplay + ". Connect's KIP-585 predicate framework "
                                + "iterates ONLY the aliases in the predicates= chain — every "
                                + "predicates." + alias + ".<key> entry is SILENTLY IGNORED at "
                                + "runtime (no error log, no warning, no ConfigException). The "
                                + "predicate is NOT registered, so any SMT referencing it via "
                                + "transforms.<smt>.predicate=" + alias + " would fail at "
                                + "startup; and if no SMT references it, the config is dead "
                                + "noise that misleads readers into believing a predicate is in "
                                + "effect when none is. Either add '" + alias + "' to the "
                                + "predicates= chain (so the predicate actually registers), OR "
                                + "remove the orphan predicates." + alias + ".* entries (so the "
                                + "config reflects the actual runtime behavior — no predicate "
                                + "registered)."));
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
