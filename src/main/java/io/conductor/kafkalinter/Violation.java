package io.conductor.kafkalinter;

/**
 * A single lint hit.
 *
 * <p>Carries only the location and the rule-specific {@code detail} string; everything
 * about <em>what kind of defect</em> (severity, confidence, doc path, category) lives
 * on the {@link RuleId} and is reachable via {@link #rule()}. That keeps the call
 * sites in concrete rules short and lets us enrich the reporter output without
 * touching every rule implementation.
 */
public record Violation(
        RuleId rule,
        Severity severity,
        String className,
        String methodName,
        int line,
        String detail) {

    public Confidence confidence() {
        return rule.confidence();
    }

    public String category() {
        return rule.category();
    }

    public String docPath() {
        return rule.docPath();
    }

    public String prettyClassName() {
        // ASM internal class names use '/' as separator and never contain a file extension
        // dot in the last segment. Filesystem paths (project-scoped rules emit those) do.
        // Don't mangle paths into ".dotted.garbage".
        if (looksLikeFilePath(className)) {
            return className;
        }
        return className.replace('/', '.');
    }

    private static boolean looksLikeFilePath(String name) {
        if (name == null || name.isEmpty()) return false;
        int lastSlash = name.lastIndexOf('/');
        if (lastSlash < 0) return false;
        String tail = name.substring(lastSlash + 1);
        return tail.indexOf('.') >= 0;
    }

    public String location() {
        String loc = prettyClassName();
        if (methodName != null && !methodName.isEmpty()) {
            loc = loc + "#" + methodName;
        }
        if (line > 0) {
            loc = loc + ":" + line;
        }
        return loc;
    }

    public String message() {
        return (detail == null || detail.isEmpty()) ? rule.message() : detail;
    }

    /** Single-line compact form — kept for callers that just want the legacy string. */
    public String format() {
        return "[" + severity + "] " + rule.id() + " " + location() + " — " + message();
    }
}
