package io.conductor.kafkalinter.rules.config;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Generic "flat properties-file key has a bad value" detector.
 *
 * <p>Walks every {@code .properties} file loaded by {@link ProjectContext}; for
 * every {@code key=value} pair whose key matches the watched key exactly AND
 * whose value passes the bad-value predicate, emits one violation per file.
 *
 * <p>For SmallRye channel-template keys
 * ({@code mp.messaging.{incoming|outgoing}.<channel>.<setting>}) use
 * {@code SmallRyeChannelConfigRule} instead — it parses the channel name out
 * and reports it in the location.
 */
public final class PropertyFileRule implements ProjectScopedRule {

    private final RuleId ruleId;
    private final Severity severity;
    private final String watchedKey;
    private final Predicate<String> badValue;
    private final String detailTemplate;
    /** Optional gate: only fire when this artefact is on the project's classpath. */
    private final String requiredGroupId;
    private final String requiredArtifactId;

    public PropertyFileRule(RuleId ruleId, Severity severity, String watchedKey,
                            Predicate<String> badValue, String detailTemplate,
                            String requiredGroupId, String requiredArtifactId) {
        this.ruleId = ruleId;
        this.severity = severity;
        this.watchedKey = watchedKey;
        this.badValue = badValue;
        this.detailTemplate = detailTemplate;
        this.requiredGroupId = requiredGroupId;
        this.requiredArtifactId = requiredArtifactId;
    }

    @Override
    public RuleId id() {
        return ruleId;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        if (requiredGroupId != null && requiredArtifactId != null
                && ctx.artifact(requiredGroupId, requiredArtifactId).isEmpty()) {
            return List.of();
        }
        List<Violation> out = new ArrayList<>();
        for (ProjectContext.PropertyHit hit : ctx.allPropertyHits()) {
            if (!watchedKey.equals(hit.key())) continue;
            String value = hit.value() == null ? "" : hit.value().trim();
            if (!badValue.test(value)) continue;
            String detail = detailTemplate.replace("{value}", value).replace("{key}", watchedKey);
            out.add(new Violation(ruleId, severity,
                    ctx.relativize(hit.file()),
                    "key:" + watchedKey, 0, detail));
        }
        return out;
    }

    public static PropertyFileRule literal(RuleId id, Severity sev, String key, String badLiteral,
                                           String detail, String groupId, String artifactId) {
        return new PropertyFileRule(id, sev, key, badLiteral::equalsIgnoreCase, detail, groupId, artifactId);
    }

    public static PropertyFileRule predicate(RuleId id, Severity sev, String key,
                                             Predicate<String> badValue, String detail,
                                             String groupId, String artifactId) {
        return new PropertyFileRule(id, sev, key, badValue, detail, groupId, artifactId);
    }
}
