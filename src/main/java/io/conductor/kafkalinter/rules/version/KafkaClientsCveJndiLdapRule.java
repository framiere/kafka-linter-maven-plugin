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
 * kafka-clients versions affected by the JNDI/LDAP RCE class of CVEs (Log4Shell-shape, plus
 * SASL/login-module-callout variants). The fix lands in 3.5.2 / 3.6.1 — anything earlier
 * in the 3.x line is reported as ERROR.
 */
public final class KafkaClientsCveJndiLdapRule implements ProjectScopedRule {

    private static final SemVer SAFE_FLOOR = new SemVer(3, 5, 2);

    private final Severity severity;

    public KafkaClientsCveJndiLdapRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.KAFKA_CLIENTS_CVE_JNDI_LDAP;
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
                RuleId.KAFKA_CLIENTS_CVE_JNDI_LDAP, severity,
                "pom.xml", "dependency:org.apache.kafka:kafka-clients", 0,
                "kafka-clients " + v + " is vulnerable to JNDI/LDAP CVE-class — upgrade to >= " + SAFE_FLOOR + "."));
    }
}
