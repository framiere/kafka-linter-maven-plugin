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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * has BOTH:
 * <ul>
 *   <li>a {@code connector.class} key (the canonical Connect-config fingerprint), AND</li>
 *   <li>at least one config value containing a KIP-297 substitution of the
 *       form {@code ${<provider-alias>:<path>:<key>}} where
 *       {@code <provider-alias>} is NOT declared in the connector's own
 *       {@code config.providers=} chain.</li>
 * </ul>
 *
 * <p>KIP-297 (Kafka 2.0+) introduced indirect configuration: connector values
 * containing {@code ${alias:path:key}} are dereferenced via a named
 * {@code ConfigProvider} plugin before the connector receives them. Connect
 * silently skips substitution when the alias is not declared — the literal
 * {@code ${...}} string is passed through, causing opaque downstream-system
 * authentication errors.
 *
 * <p>Emits one violation per offending (file, undeclared alias) pair.
 */
public final class ConnectConfigProviderReferenceUndefinedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String CONFIG_PROVIDERS = "config.providers";

    // Matches ${alias:...} where alias is a non-empty run of word/dash chars.
    // The path/key portion after the first colon is opaque to us (may contain
    // colons, slashes, etc.); we only care about the leading alias.
    private static final Pattern PROVIDER_REF = Pattern.compile("\\$\\{([A-Za-z0-9_-]+):[^}]*}");

    private final Severity severity;

    public ConnectConfigProviderReferenceUndefinedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_CONFIG_PROVIDER_REFERENCE_UNDEFINED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            Set<String> declared = parseChain(p.getProperty(CONFIG_PROVIDERS));

            // Aggregate undefined aliases and the first key that references each.
            Map<String, String> undefinedToKey = new LinkedHashMap<>();
            for (String key : new TreeSet<>(p.stringPropertyNames())) {
                String value = p.getProperty(key);
                if (value == null || value.indexOf("${") < 0) continue;
                Matcher m = PROVIDER_REF.matcher(value);
                while (m.find()) {
                    String alias = m.group(1);
                    if (declared.contains(alias)) continue;
                    undefinedToKey.putIfAbsent(alias, key);
                }
            }

            for (Map.Entry<String, String> ref : undefinedToKey.entrySet()) {
                String alias = ref.getKey();
                String firstKey = ref.getValue();
                out.add(new Violation(
                        RuleId.CONNECT_CONFIG_PROVIDER_REFERENCE_UNDEFINED, severity,
                        ctx.relativize(e.getKey()), "key:" + firstKey, 0,
                        "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                                + " uses ${" + alias + ":...} substitution (first seen in "
                                + firstKey + ") but '" + alias
                                + "' is NOT declared in config.providers="
                                + (p.getProperty(CONFIG_PROVIDERS) == null
                                        ? "<unset>" : p.getProperty(CONFIG_PROVIDERS).trim())
                                + " — Connect SILENTLY SKIPS the substitution; the literal "
                                + "${" + alias + ":...} string is passed to the connector and "
                                + "the downstream system rejects authentication (or, worse, "
                                + "silently uses the literal as a credential). Add '" + alias
                                + "' to config.providers= and bind it via config.providers."
                                + alias + ".class=<ConfigProvider-class> "
                                + "(e.g., org.apache.kafka.common.config.provider.FileConfigProvider, "
                                + "...EnvVarConfigProvider)."));
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
