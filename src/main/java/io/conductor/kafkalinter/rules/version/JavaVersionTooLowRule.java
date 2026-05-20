package io.conductor.kafkalinter.rules.version;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.util.List;
import java.util.Optional;

/**
 * Flags projects targeting Java &lt; 11 when a kafka-clients (or framework) dependency is
 * present — modern Kafka requires Java 11+.
 *
 * <p>The check only fires when the project actually depends on kafka-clients; otherwise
 * the rule is irrelevant. The lint catches the "kafka-clients 3.x on a Java 8 base" mistake
 * specifically.
 */
public final class JavaVersionTooLowRule implements ProjectScopedRule {

    private static final int MIN_JAVA = 11;

    private final Severity severity;

    public JavaVersionTooLowRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.JAVA_VERSION_TOO_LOW;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        if (ctx.artifact("org.apache.kafka", "kafka-clients").isEmpty()
                && ctx.artifact("org.apache.kafka", "kafka-streams").isEmpty()) {
            return List.of();
        }
        Optional<String> target = ctx.javaTargetVersion();
        if (target.isEmpty()) return List.of();
        Integer parsed = parseJavaVersion(target.get());
        if (parsed == null || parsed >= MIN_JAVA) return List.of();

        return List.of(new Violation(
                RuleId.JAVA_VERSION_TOO_LOW, severity,
                "pom.xml", "property:maven.compiler.target", 0,
                "Project targets Java " + parsed + " — kafka-clients 3.x requires Java >= " + MIN_JAVA + "."));
    }

    /** Accepts "1.8", "8", "11", "17", "21" — returns the canonical integer or null. */
    static Integer parseJavaVersion(String raw) {
        if (raw == null) return null;
        String s = raw.trim();
        if (s.startsWith("1.")) {
            try { return Integer.parseInt(s.substring(2)); }
            catch (NumberFormatException ignored) { return null; }
        }
        try { return Integer.parseInt(s); }
        catch (NumberFormatException ignored) { return null; }
    }
}
