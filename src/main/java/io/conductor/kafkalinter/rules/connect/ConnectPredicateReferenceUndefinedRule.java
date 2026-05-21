package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * has BOTH:
 * <ul>
 *   <li>a {@code connector.class} key (the canonical Connect-config fingerprint), AND</li>
 *   <li>at least one {@code transforms.<smt-alias>.predicate=<predicate-alias>}
 *       reference where the referenced predicate alias is EITHER missing from
 *       the connector's {@code predicates=} chain OR has a missing/blank
 *       {@code predicates.<predicate-alias>.type} binding.</li>
 * </ul>
 *
 * <p>KIP-585 (Kafka 2.6+) introduced conditional SMTs: an SMT can be gated on
 * a predicate. Every predicate alias used in a
 * {@code transforms.<alias>.predicate=<...>} reference must be both
 * (a) listed in the {@code predicates=} chain AND (b) bound to a class via
 * {@code predicates.<alias>.type=<Predicate-class>}.
 *
 * <p>Emits one violation per offending {@code transforms.<smt-alias>.predicate}
 * reference (so a file with three dangling references emits three violations).
 */
public final class ConnectPredicateReferenceUndefinedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PREDICATES_LIST = "predicates";
    private static final String TRANSFORMS_PREFIX = "transforms.";
    private static final String PREDICATE_SUFFIX = ".predicate";

    private final Severity severity;

    public ConnectPredicateReferenceUndefinedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_PREDICATE_REFERENCE_UNDEFINED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            Set<String> declaredPredicates = parseChain(p.getProperty(PREDICATES_LIST));

            for (String keyObj : p.stringPropertyNames()) {
                if (!keyObj.startsWith(TRANSFORMS_PREFIX)) continue;
                if (!keyObj.endsWith(PREDICATE_SUFFIX)) continue;

                String smtAlias = keyObj.substring(
                        TRANSFORMS_PREFIX.length(),
                        keyObj.length() - PREDICATE_SUFFIX.length());
                if (smtAlias.isEmpty() || smtAlias.contains(".")) continue;

                String predicateRefRaw = p.getProperty(keyObj);
                if (!notBlank(predicateRefRaw)) continue;
                String predicateRef = predicateRefRaw.trim();

                boolean inChain = declaredPredicates.contains(predicateRef);
                boolean hasType = notBlank(p.getProperty("predicates." + predicateRef + ".type"));
                if (inChain && hasType) continue;

                String reason;
                if (!inChain && !hasType) {
                    reason = "neither listed in predicates= chain nor defined via predicates."
                            + predicateRef + ".type";
                } else if (!inChain) {
                    reason = "not listed in the predicates= chain "
                            + "(predicates=" + (p.getProperty(PREDICATES_LIST) == null ? "<unset>"
                                    : p.getProperty(PREDICATES_LIST).trim()) + ")";
                } else {
                    reason = "predicates." + predicateRef + ".type is missing or blank";
                }

                out.add(new Violation(
                        RuleId.CONNECT_PREDICATE_REFERENCE_UNDEFINED, severity,
                        ctx.relativize(e.getKey()), "key:" + keyObj, 0,
                        "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                                + " references predicate alias '" + predicateRef
                                + "' via " + keyObj + " but " + reason
                                + " — the task will fail at startup with ConfigException. "
                                + "Either add predicates." + predicateRef + ".type=<Predicate-class> "
                                + "and ensure '" + predicateRef + "' is listed in the predicates= "
                                + "chain, or remove the " + keyObj + " reference."));
            }
        }
        return out;
    }

    private static Set<String> parseChain(String raw) {
        if (!notBlank(raw)) return Set.of();
        Set<String> out = new HashSet<>();
        for (String token : Arrays.asList(raw.split(","))) {
            String t = token.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
