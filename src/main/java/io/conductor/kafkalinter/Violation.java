package io.conductor.kafkalinter;

public record Violation(
        RuleId rule,
        Severity severity,
        String className,
        String methodName,
        int line,
        String detail) {

    public String format() {
        String loc = className.replace('/', '.') + "#" + methodName + (line > 0 ? ":" + line : "");
        return "[" + severity + "] " + rule.name() + " " + loc + " — " + (detail == null || detail.isEmpty() ? rule.message() : detail);
    }
}
