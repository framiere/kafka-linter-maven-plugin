package io.conductor.kafkalinter.scanner;

import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs every {@link ProjectScopedRule} against a shared {@link ProjectContext} snapshot.
 *
 * <p>Counterpart to {@link ProjectScanner}, which walks classes and runs class-scoped
 * {@link io.conductor.kafkalinter.rules.Rule} instances.
 */
public final class ProjectRuleRunner {

    private final List<ProjectScopedRule> rules;

    public ProjectRuleRunner(List<ProjectScopedRule> rules) {
        this.rules = rules;
    }

    public List<Violation> run(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (ProjectScopedRule r : rules) {
            out.addAll(r.check(ctx));
        }
        return out;
    }
}
