package io.conductor.kafkalinter.rules.version;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;
import org.apache.maven.artifact.Artifact;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Generic kafka-clients CVE version-range detector.
 *
 * <p>Looks at the resolved {@code org.apache.kafka:kafka-clients} version in the
 * project and fires when the supplied {@code vulnerable} predicate matches. Used
 * for CVEs that don't fit a single "below safe floor" shape — e.g. ones patched
 * in multiple lines (3.9.2 + 4.0.2 + 4.1.2).
 */
public final class KafkaClientsCveRule implements ProjectScopedRule {

    private final RuleId ruleId;
    private final Severity severity;
    private final Predicate<SemVer> vulnerable;
    private final String detail;

    public KafkaClientsCveRule(RuleId ruleId, Severity severity,
                               Predicate<SemVer> vulnerable, String detail) {
        this.ruleId = ruleId;
        this.severity = severity;
        this.vulnerable = vulnerable;
        this.detail = detail;
    }

    @Override
    public RuleId id() {
        return ruleId;
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
        if (!vulnerable.test(v)) return List.of();
        return List.of(new Violation(
                ruleId, severity,
                "pom.xml", "dependency:org.apache.kafka:kafka-clients", 0,
                "kafka-clients " + v + " " + detail));
    }
}
