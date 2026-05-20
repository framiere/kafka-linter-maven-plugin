package io.conductor.kafkalinter.rules.version;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.util.List;

/**
 * Quarkus 3 renamed {@code quarkus-smallrye-reactive-messaging-kafka} (and the older
 * {@code quarkus-kafka}) to {@code quarkus-messaging-kafka}. The old artifacts still
 * resolve but miss new config bindings.
 */
public final class QuarkusKafkaExtensionRenamedRule implements ProjectScopedRule {

    private final Severity severity;

    public QuarkusKafkaExtensionRenamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.QUARKUS_KAFKA_EXTENSION_RENAMED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        // Fire only on a Quarkus 3+ project (look for the modern messaging artifact OR
        // a quarkus.platform.version property hinting at 3.x). To stay simple, the lint
        // fires whenever one of the old artifacts is present — Quarkus 2 is itself EOL.
        boolean hasOldSmallrye = ctx.artifact("io.quarkus", "quarkus-smallrye-reactive-messaging-kafka").isPresent();
        boolean hasOldKafka = ctx.artifact("io.quarkus", "quarkus-kafka").isPresent();
        if (!hasOldSmallrye && !hasOldKafka) return List.of();

        String oldName = hasOldSmallrye ? "quarkus-smallrye-reactive-messaging-kafka" : "quarkus-kafka";
        return List.of(new Violation(
                RuleId.QUARKUS_KAFKA_EXTENSION_RENAMED, severity,
                "pom.xml", "dependency:io.quarkus:" + oldName, 0,
                "Deprecated artifact io.quarkus:" + oldName + " — replace with io.quarkus:quarkus-messaging-kafka."));
    }
}
