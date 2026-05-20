package io.conductor.kafkalinter.rules.version;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;
import org.apache.maven.artifact.Artifact;

import java.util.List;
import java.util.Optional;

/**
 * Flags kafka-clients versions vulnerable to the SASL/OAUTHBEARER audience-validation
 * flaw. Fix landed in 3.6.2 / 3.7.0.
 */
public final class KafkaClientsCveSaslOAuthRule implements ProjectScopedRule {

    private static final SemVer SAFE_FLOOR = new SemVer(3, 6, 2);

    private final Severity severity;

    public KafkaClientsCveSaslOAuthRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Optional<Artifact> a = ctx.artifact("org.apache.kafka", "kafka-clients");
        if (a.isEmpty()) return List.of();
        SemVer v;
        try {
            v = SemVer.parse(a.get().getBaseVersion());
        } catch (IllegalArgumentException ex) {
            return List.of();
        }
        if (v.atLeast(SAFE_FLOOR)) return List.of();

        return List.of(new Violation(
                RuleId.KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER, severity,
                "pom.xml", "dependency:org.apache.kafka:kafka-clients", 0,
                "kafka-clients " + v + " has the SASL/OAUTHBEARER audience-validation flaw — upgrade to >= " + SAFE_FLOOR + "."));
    }
}
