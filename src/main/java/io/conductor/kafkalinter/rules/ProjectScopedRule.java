package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.util.List;

/**
 * A rule that operates on the whole Maven project, not on individual classes.
 *
 * <p>Useful for pom-dependency checks (CVE-affected version ranges, end-of-life
 * artifacts, framework artifact renames) and for project-wide config-file
 * inspection (application.properties / application.yaml under src/main/resources).
 *
 * <p>The bytecode-class oriented sibling is {@link Rule}.
 */
public interface ProjectScopedRule {
    RuleId id();
    List<Violation> check(ProjectContext ctx);
}
