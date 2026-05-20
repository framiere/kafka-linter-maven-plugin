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
 * Flags Spring Boot 3.x with spring-kafka outside the expected 3.x range (Spring Boot
 * 3 BOM ships spring-kafka 3.x — pairing it with 2.x is the common mismatch).
 *
 * <p>Detection is intentionally narrow: we only flag the clearly-mismatched case
 * (Boot 3.x + spring-kafka 2.x) to avoid false positives across BOM versions we
 * don't know about. The detail line names both versions for fast triage.
 */
public final class SpringKafkaBootMismatchRule implements ProjectScopedRule {

    private final Severity severity;

    public SpringKafkaBootMismatchRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SPRING_KAFKA_BOOT_MISMATCH;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Optional<Artifact> boot = ctx.artifact("org.springframework.boot", "spring-boot");
        Optional<Artifact> kafka = ctx.artifact("org.springframework.kafka", "spring-kafka");
        if (boot.isEmpty() || kafka.isEmpty()) return List.of();

        SemVer bootV, kafkaV;
        try {
            bootV = SemVer.parse(boot.get().getBaseVersion());
            kafkaV = SemVer.parse(kafka.get().getBaseVersion());
        } catch (IllegalArgumentException ex) {
            return List.of();
        }

        // Boot 3.x ⇒ spring-kafka 3.x. Boot 2.x ⇒ spring-kafka 2.x.
        if (bootV.major() != kafkaV.major()) {
            return List.of(new Violation(
                    RuleId.SPRING_KAFKA_BOOT_MISMATCH, severity,
                    "pom.xml", "dependency:org.springframework.kafka:spring-kafka", 0,
                    "spring-boot " + bootV + " paired with spring-kafka " + kafkaV
                            + " — major versions must match (Boot 3.x ⇒ spring-kafka 3.x)."));
        }
        return List.of();
    }
}
