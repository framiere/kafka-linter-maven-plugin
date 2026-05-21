package io.conductor.kafkalinter.rules.clients;

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
import java.util.Set;

/**
 * Flags property files whose bootstrap host matches a known managed Kafka
 * cluster suffix (Confluent Cloud, MSK, Aiven, Redpanda Cloud, Event Hubs)
 * while {@code security.protocol} is not configured. Explicit
 * {@code security.protocol=PLAINTEXT} is handled by SECURITY_PROTOCOL_PLAINTEXT.
 */
public final class SecurityProtocolPlaintextRemoteRule implements ProjectScopedRule {

    private static final Set<String> MANAGED_SUFFIXES = Set.of(
            ".confluent.cloud",
            ".amazonaws.com",
            ".aivencloud.com",
            ".cloud.redpanda.com",
            ".servicebus.windows.net");
    private static final String PLAIN_BOOTSTRAP_KEY = "bootstrap.servers";
    private static final String PLAIN_SECURITY_KEY = "security.protocol";
    private static final String SPRING_BOOTSTRAP_KEY = "spring.kafka.bootstrap-servers";
    private static final String SPRING_SECURITY_KEY = "spring.kafka.properties.security.protocol";

    private final Severity severity;

    public SecurityProtocolPlaintextRemoteRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SECURITY_PROTOCOL_PLAINTEXT_REMOTE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            checkPair(out, ctx, e.getKey(), p, PLAIN_BOOTSTRAP_KEY, PLAIN_SECURITY_KEY);
            checkPair(out, ctx, e.getKey(), p, SPRING_BOOTSTRAP_KEY, SPRING_SECURITY_KEY);
        }
        return out;
    }

    private void checkPair(List<Violation> out, ProjectContext ctx, Path file, Properties p,
                           String bootstrapKey, String securityKey) {
        String bootstrap = p.getProperty(bootstrapKey);
        if (bootstrap == null || bootstrap.isBlank()) return;
        String managedSuffix = matchManagedSuffix(bootstrap);
        if (managedSuffix == null) return;
        String securityProtocol = p.getProperty(securityKey);
        if (securityProtocol != null && !securityProtocol.isBlank()) return;
        out.add(new Violation(
                RuleId.SECURITY_PROTOCOL_PLAINTEXT_REMOTE, severity,
                ctx.relativize(file), "key:" + bootstrapKey, 0,
                bootstrapKey + "=" + bootstrap.trim()
                        + " — managed cluster suffix \"" + managedSuffix
                        + "\" but " + securityKey + " is unset (client defaults to PLAINTEXT). Set "
                        + securityKey + "=SASL_SSL (or SSL for mTLS) and the matching sasl.* / ssl.* keys."));
    }

    private static String matchManagedSuffix(String bootstrap) {
        String lower = bootstrap.toLowerCase();
        for (String host : lower.split(",")) {
            int colon = host.indexOf(':');
            String h = (colon >= 0 ? host.substring(0, colon) : host).trim();
            for (String suffix : MANAGED_SUFFIXES) {
                if (h.endsWith(suffix)) return suffix;
            }
        }
        return null;
    }
}
