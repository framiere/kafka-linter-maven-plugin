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
 * Flags {@code org.apache.kafka:kafka-clients} versions that have fallen off Apache Kafka's
 * support window (only the latest two minors get patches in practice).
 */
public final class KafkaClientsEolRule implements ProjectScopedRule {

    private static final SemVer EOL_FLOOR = new SemVer(3, 5, 0);

    private final Severity severity;

    public KafkaClientsEolRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.KAFKA_CLIENTS_EOL;
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
        if (v.atLeast(EOL_FLOOR)) return List.of();

        return List.of(new Violation(
                RuleId.KAFKA_CLIENTS_EOL, severity,
                "pom.xml", "dependency:org.apache.kafka:kafka-clients", 0,
                "kafka-clients " + v + " is past EOL — upgrade to >= " + EOL_FLOOR + "."));
    }
}
