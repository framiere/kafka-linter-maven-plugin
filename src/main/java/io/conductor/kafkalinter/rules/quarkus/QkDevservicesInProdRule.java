package io.conductor.kafkalinter.rules.quarkus;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Flags a Quarkus {@code application.properties} that does NOT explicitly disable
 * DevServices for Kafka in the {@code %prod} profile.
 *
 * <p>The Quarkus DevServices integration spawns a Testcontainers Kafka broker when no
 * {@code kafka.bootstrap.servers} is configured. In dev/test that's a feature; in
 * production it means the application boots against a throwaway container and silently
 * ignores the real cluster. The defensive config is
 * {@code %prod.quarkus.kafka.devservices.enabled=false}.
 *
 * <p>To avoid noise we only fire when:
 * <ul>
 *   <li>the project depends on a Quarkus Kafka artifact (extension present), AND</li>
 *   <li>at least one application.properties exists that mentions {@code quarkus.kafka}
 *       or {@code %prod.kafka} but does NOT set {@code %prod.quarkus.kafka.devservices.enabled=false}.</li>
 * </ul>
 */
public final class QkDevservicesInProdRule implements ProjectScopedRule {

    private static final String GUARD_KEY = "%prod.quarkus.kafka.devservices.enabled";

    private final Severity severity;

    public QkDevservicesInProdRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.QK_DEVSERVICES_IN_PROD;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        if (!isQuarkusKafkaProject(ctx)) return List.of();

        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, java.util.Properties> e : ctx.propertiesFiles().entrySet()) {
            Path file = e.getKey();
            java.util.Properties props = e.getValue();
            if (!file.getFileName().toString().startsWith("application")) continue;
            String guard = props.getProperty(GUARD_KEY);
            if ("false".equalsIgnoreCase(String.valueOf(guard).trim())) continue;
            out.add(new Violation(
                    RuleId.QK_DEVSERVICES_IN_PROD, severity,
                    ctx.relativize(file), "configuration", 0,
                    "Quarkus Kafka DevServices is not explicitly disabled for the %prod profile — set "
                            + GUARD_KEY + "=false to be safe."));
        }
        return out;
    }

    private static boolean isQuarkusKafkaProject(ProjectContext ctx) {
        return ctx.artifact("io.quarkus", "quarkus-messaging-kafka").isPresent()
                || ctx.artifact("io.quarkus", "quarkus-smallrye-reactive-messaging-kafka").isPresent()
                || ctx.artifact("io.quarkus", "quarkus-kafka-client").isPresent()
                || ctx.artifact("io.quarkus", "quarkus-kafka").isPresent();
    }
}
